"""Guard: the Spark SQL emitters this project controls must never emit a bare
``CAST(``. Databricks warehouses run with ANSI mode on, where a bare ``CAST``
aborts the whole statement on the first value that will not convert;
``TRY_CAST`` yields NULL instead. See
``docs/superpowers/specs/2026-08-05-spark-try-cast-design.md``.

Deliberately excluded (see the design doc's "Amendment" section):
``src/jobs/graph_analytics_job.py`` and
``src/back/core/graph_analysis/JobMetrics.py`` — their SQL runs literally
against SQLite in tests and is parse-checked under Postgres, neither of which
understands ``TRY_CAST``, and every cast there is on already-numeric data
(counts, literals, typed NULLs), never raw triple-store strings.
"""

import pytest

pytestmark = pytest.mark.unit


def _no_bare_cast(sql: str) -> bool:
    return "CAST(" not in sql.replace("TRY_CAST(", "")


class TestSqlHelpers:
    def test_sql_cast_has_no_bare_cast(self):
        from back.core.helpers.SQLHelpers import SQLHelpers

        assert _no_bare_cast(SQLHelpers.sql_cast("x", "DOUBLE"))


class TestSparqlTranslator:
    def test_cast_str_has_no_bare_cast(self):
        from back.core.w3c.sparql.SparqlTranslator import SparqlTranslator

        assert _no_bare_cast(SparqlTranslator._cast_str("col"))

    def test_coalesce_cast_str_has_no_bare_cast(self):
        from back.core.w3c.sparql.SparqlTranslator import SparqlTranslator

        assert _no_bare_cast(SparqlTranslator._coalesce_cast_str("col"))

    def test_subject_expr_from_template_has_no_bare_cast(self):
        from back.core.w3c.sparql.SparqlTranslator import SparqlTranslator

        result = SparqlTranslator._subject_expr_from_template(
            "http://ex.org/Customer/{id}", "customer_id", alias="c"
        )
        assert "TRY_CAST(c.customer_id AS STRING)" in result
        assert _no_bare_cast(result)


class TestAbstractUnionMapping:
    def test_null_column_has_no_bare_cast(self):
        from agents.agent_mapping_pge import coverage as cov

        abstract_entity = {"name": "Patient", "attributes": [{"name": "postcode"}]}
        subclass_mapping = {
            "ontology_class": "u#Baby",
            "id_column": "ID",
            "sql_query": "SELECT nhs AS ID FROM c.s.baby",
            "attribute_mappings": {},
        }
        m = cov.build_abstract_union_mapping("u#Patient", abstract_entity, [subclass_mapping])
        assert _no_bare_cast(m["sql_query"])


class TestLakeflowWrap:
    def test_object_hash_expression_has_no_bare_cast(self):
        from back.core.graphdb.lakebase._companion_ddl import (
            wrap_triple_view_sql_for_lakeflow,
        )

        wrapped = wrap_triple_view_sql_for_lakeflow("SELECT 1 AS object")
        assert _no_bare_cast(wrapped)
