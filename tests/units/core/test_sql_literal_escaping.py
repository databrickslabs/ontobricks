"""SQL string literals built by the SQL-emitting engines stay closed.

Spark treats a backslash as an escape inside ``'...'``, so a value ending in
``\\`` breaks out of a literal that only doubles quotes. Postgres (with
``standard_conforming_strings``) keeps backslashes verbatim.
"""

from unittest.mock import MagicMock

import pytest

from back.core.graphdb.delta.MaterializedStore import MaterializedStore
from back.core.graphdb.lakebase.FlatStore import FlatStore
from back.core.helpers import SQLHelpers
from back.core.reasoning.rules.SPARQLRuleEngine import SPARQLRuleEngine
from back.core.reasoning.shacl_rules.ShaclFlatSqlBackend import ShaclFlatSqlBackend
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
    """SPARQL IRIs cannot hold spaces or backslashes, so IRI payloads use a
    quote only; string literals carry the full payload (``\\\\`` in SPARQL)."""

    IRI_PAYLOAD = "x'OR'1'='1"
    SPARQL_STRING = PAYLOAD.replace("\\", "\\\\")

    @staticmethod
    def _sql(where: str, dialect: str = "spark") -> str:
        query = (
            f"CONSTRUCT {{ ?p rdf:type <http://test.org/{TestSPARQLRuleEngine.IRI_PAYLOAD}> }} "
            f"WHERE {{ ?p rdf:type <{CLS}> . ?p <http://test.org/amt> ?amt . {where} }}"
        )
        store = MagicMock()
        store.sql_flavor.return_value = dialect
        store.sql_table_reference.return_value = "triples"
        store.execute_query.return_value = []
        SPARQLRuleEngine()._execute_one("r", query, store, "t", {"base_uri": BASE})
        return store.execute_query.call_args.args[0]

    def test_construct_iri_stays_closed(self):
        assert f"http://test.org/{self.IRI_PAYLOAD}" in _literals(self._sql(""))

    def test_filter_string_stays_closed(self):
        sql = self._sql(f'FILTER(?amt = "{self.SPARQL_STRING}")')
        assert PAYLOAD in _literals(sql)

    def test_postgres_dialect_keeps_backslash(self):
        sql = self._sql(f'FILTER(?amt = "{self.SPARQL_STRING}")', dialect="postgres")
        assert PAYLOAD in _literals(sql, "postgres")

    def test_like_filter_stays_closed(self):
        sql = self._sql(f'FILTER(CONTAINS(?amt, "{self.SPARQL_STRING}"))')
        assert any(PAYLOAD in lit for lit in _literals(sql))

    def test_numeric_filter_is_cast(self):
        sql = self._sql("FILTER(?amt > 100000)")
        assert "TRY_CAST(t2.object AS DOUBLE) > 100000" in sql


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


class TestGraphBackendEscape:
    def test_delta_store_escapes_backslash(self):
        store = MaterializedStore.__new__(MaterializedStore)
        assert store._sql_escape(PAYLOAD) == SQLHelpers.sql_escape(PAYLOAD, "spark")

    def test_lakebase_store_keeps_backslash(self):
        store = FlatStore.__new__(FlatStore)
        assert store._sql_escape(PAYLOAD) == SQLHelpers.sql_escape(PAYLOAD, "postgres")


class TestShaclFlatSqlBackendEscape:
    def test_escape_uses_canonical_helper(self):
        assert ShaclFlatSqlBackend._escape(PAYLOAD) == SQLHelpers.sql_escape(PAYLOAD)
