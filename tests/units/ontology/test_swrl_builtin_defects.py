"""Regression tests for the four SWRL builtin defects (found 2026-09).

The rule editor emits ``swrlb:``-prefixed builtins over datatype
properties; four independent defects made every such rule silently infer
zero triples. See scripts/repro_swrl_builtin_defects.py for the
end-to-end red/green harness.
"""

from back.core.reasoning.SWRLBuiltinRegistry import SWRLBuiltinRegistry
from back.core.reasoning.SWRLEngine import SWRLEngine
from back.core.reasoning.SWRLSQLTranslator import SWRLSQLTranslator

BASE = "https://example.com/PCP#"
DATA = "https://example.com/PCP/"

ONTOLOGY = {
    "base_uri": BASE,
    "classes": [
        {
            "name": "Sailing",
            "uri": BASE + "Sailing",
            "dataProperties": [{"name": "nights", "uri": BASE + "nights"}],
        },
        {"name": "LongSailing", "uri": BASE + "LongSailing", "dataProperties": []},
    ],
    "properties": [{"name": "operatedBy", "uri": BASE + "operatedBy"}],
}


def _params():
    return {
        "antecedent": (
            "Sailing(?x) ∧ nights(?x, ?nights) "
            "∧ swrlb:greaterThanOrEqual(?nights, 10)"
        ),
        "consequent": "LongSailing(?x)",
        "base_uri": BASE,
        "uri_map": SWRLEngine(ONTOLOGY)._build_uri_map(),
    }


class TestDefect1BuiltinPrefix:
    def test_editor_prefixed_name_is_recognised(self):
        assert SWRLBuiltinRegistry.is_builtin("swrlb:greaterThanOrEqual")
        assert SWRLBuiltinRegistry.get("swrlb:greaterThanOrEqual") is not None

    def test_bare_and_cased_names_still_work(self):
        assert SWRLBuiltinRegistry.is_builtin("greaterThanOrEqual")
        assert SWRLBuiltinRegistry.is_builtin("GREATERTHANOREQUAL")

    def test_non_builtin_stays_false(self):
        assert not SWRLBuiltinRegistry.is_builtin("swrlb:notARealBuiltin")
        assert not SWRLBuiltinRegistry.is_builtin("operatedBy")


class TestDefect2UriMapDataProperties:
    def test_datatype_properties_get_data_namespace(self):
        uri_map = SWRLEngine(ONTOLOGY)._build_uri_map()
        assert uri_map["sailing"] == BASE + "Sailing"  # classes keep '#'
        assert uri_map["operatedby"] == DATA + "operatedBy"
        assert uri_map["nights"] == DATA + "nights"


class TestDefect3BuiltinsAreFiltersNotJoins:
    def test_inference_sql_emits_where_filter(self):
        sql = SWRLSQLTranslator().build_inference_sql("t", _params())
        assert sql is not None
        assert "greaterThanOrEqual" not in sql  # no phantom predicate join
        assert f"predicate = '{DATA}nights'" in sql
        assert "TRY_CAST" in sql and ">=" in sql

    def test_materialization_sql_emits_where_filter(self):
        sql = SWRLSQLTranslator().build_materialization_sql("t", _params())
        assert sql is not None
        assert "greaterThanOrEqual" not in sql
        assert "TRY_CAST" in sql and ">=" in sql


class TestDefect4PostgresDialect:
    def test_postgres_dialect_maps_cast_and_double(self):
        sql = SWRLSQLTranslator(dialect="postgres").build_inference_sql(
            "t", _params()
        )
        assert sql is not None
        assert "TRY_CAST" not in sql
        assert "CAST(" in sql
        assert "DOUBLE PRECISION" in sql

    def test_databricks_dialect_unchanged(self):
        sql = SWRLSQLTranslator(dialect="databricks").build_inference_sql(
            "t", _params()
        )
        assert "TRY_CAST" in sql
        assert "DOUBLE PRECISION" not in sql

    def test_lakebase_backend_hands_out_postgres_dialect(self):
        from back.core.graphdb.lakebase.LakebaseBase import LakebaseBase

        translator = LakebaseBase.get_query_translator(None)
        assert isinstance(translator, SWRLSQLTranslator)
        assert translator._dialect == "postgres"
