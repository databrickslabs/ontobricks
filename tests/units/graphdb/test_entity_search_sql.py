"""SQL shape tests for the shared entity-search index helpers."""

from __future__ import annotations

import pytest

from back.core.graphdb.constants import RDF_TYPE, RDFS_LABEL
from back.core.graphdb.entity_search import (
    _entity_search_text_clause,
    entity_search_seed_sql,
    entity_search_select,
    entity_search_uri_search_sql,
    is_asserted_only_relation,
    is_missing_relation_error,
    is_view_not_table_error,
    preview_select_sql,
    sort_preview_rows,
)


pytestmark = pytest.mark.unit


def _escape(value: str) -> str:
    return value.replace("'", "''")


def test_is_missing_relation_error_matches_known_markers() -> None:
    assert is_missing_relation_error(RuntimeError("TABLE_OR_VIEW_NOT_FOUND: g")) is True
    assert is_missing_relation_error(RuntimeError("relation g does not exist")) is True
    assert is_missing_relation_error(RuntimeError("undefined table g")) is True
    assert is_missing_relation_error(RuntimeError("permission denied")) is False


def test_is_view_not_table_error_matches_describe_detail_on_a_view() -> None:
    exc = RuntimeError(
        "[EXPECT_TABLE_NOT_VIEW.NO_ALTERNATIVE] 'DESCRIBE DETAIL' expects a table "
        "but `cat`.`sch`.`g_data` is a view. SQLSTATE: 42809"
    )
    assert is_view_not_table_error(exc) is True
    assert is_missing_relation_error(exc) is False
    assert is_view_not_table_error(RuntimeError("TABLE_OR_VIEW_NOT_FOUND: g")) is False


def test_is_view_not_table_error_reads_apps_sea_detail() -> None:
    """Statement Execution wraps the warehouse error in InfrastructureError.detail."""
    from back.core.errors import InfrastructureError

    exc = InfrastructureError(
        "Databricks SQL statement failed",
        detail=(
            '{"error_code":"BAD_REQUEST","message":"[EXPECT_TABLE_NOT_VIEW.NO_ALTERNATIVE] '
            "'DESCRIBE DETAIL' expects a table but `cat`.`sch`.`g_data` is a view. "
            'SQLSTATE: 42809"}'
        ),
    )
    assert is_view_not_table_error(exc) is True
    assert is_missing_relation_error(exc) is False


def test_is_missing_relation_error_reads_apps_sea_detail() -> None:
    from back.core.errors import InfrastructureError

    exc = InfrastructureError(
        "Databricks SQL statement failed",
        detail='{"error_code":"BAD_REQUEST","message":"TABLE_OR_VIEW_NOT_FOUND: g"}',
    )
    assert is_missing_relation_error(exc) is True
    assert is_view_not_table_error(exc) is False


def test_asserted_only_suffixes() -> None:
    assert is_asserted_only_relation("cat.sch.g_data") is True
    assert is_asserted_only_relation("g_x_v1_sync") is True
    assert is_asserted_only_relation("cat.sch.g_graph") is False
    assert is_asserted_only_relation("Domain_V1") is False


def test_entity_search_text_clause_empty_value_returns_empty_string() -> None:
    assert _entity_search_text_clause(field="any", match_type="contains", value="", escape=_escape) == ""


def test_entity_search_text_clause_any_contains_both_columns() -> None:
    clause = _entity_search_text_clause(field="any", match_type="contains", value="Jac", escape=_escape)
    assert clause == "(label_lc LIKE '%jac%' ESCAPE '!' OR uri_lc LIKE '%jac%' ESCAPE '!')"


def test_entity_search_text_clause_label_exact() -> None:
    clause = _entity_search_text_clause(field="label", match_type="exact", value="Ada", escape=_escape)
    assert clause == "(label_lc = 'ada')"


def test_entity_search_text_clause_id_starts() -> None:
    clause = _entity_search_text_clause(field="id", match_type="starts", value="Cust", escape=_escape)
    assert clause == "(uri_lc LIKE 'cust%' ESCAPE '!')"


def test_entity_search_select_projects_typed_instances() -> None:
    sql = entity_search_select("g._graph")
    assert sql.count("g._graph") == 2
    assert f"predicate = '{RDF_TYPE}'" in sql
    assert f"predicate = '{RDFS_LABEL}'" in sql
    assert "AS uri" in sql
    assert "AS type_uri" in sql
    assert "AS label" in sql
    assert "AS uri_lc" in sql
    assert "AS label_lc" in sql
    assert "LEFT JOIN" in sql
    assert sql.count("GROUP BY subject") == 2


def test_preview_sql_any_contains_and_limit() -> None:
    sql = preview_select_sql(
        search_table="g_entity_search",
        entity_type="",
        field="any",
        match_type="contains",
        value="Jac",
        limit=501,
        escape=_escape,
    )
    assert "FROM g_entity_search" in sql
    assert "label_lc LIKE '%jac%' ESCAPE '!'" in sql
    assert "uri_lc LIKE '%jac%' ESCAPE '!'" in sql
    assert "ORDER BY" not in sql
    assert "LIMIT 501" in sql


def test_preview_sql_type_and_exact_label() -> None:
    sql = preview_select_sql(
        search_table="g_entity_search",
        entity_type="http://ex/Person",
        field="label",
        match_type="exact",
        value="Ada",
        limit=10,
        escape=_escape,
    )
    where = sql.split("WHERE", 1)[1].split("LIMIT", 1)[0]
    assert "type_uri = 'http://ex/Person'" in where
    assert "label_lc = 'ada'" in where
    assert "uri_lc" not in where


def test_preview_sql_rejects_non_positive_limit() -> None:
    with pytest.raises(ValueError, match="limit must be greater than zero"):
        preview_select_sql(
            search_table="g_entity_search",
            entity_type="",
            field="any",
            match_type="contains",
            value="x",
            limit=0,
            escape=_escape,
        )


def test_preview_select_sql_has_no_warehouse_order_by() -> None:
    sql = preview_select_sql(
        search_table="g_entity_search",
        entity_type="",
        field="any",
        match_type="contains",
        value="ada",
        limit=501,
        escape=_escape,
    )
    assert "ORDER BY" not in sql
    assert "LIMIT 501" in sql


def test_entity_search_uri_search_sql_exact_type_and_pagination() -> None:
    sql = entity_search_uri_search_sql(
        search_table="g_entity_search",
        type_uri="http://ex.org/Customer",
        search="",
        limit=50,
        offset=10,
        escape=_escape,
    )
    assert sql == (
        "SELECT uri FROM g_entity_search WHERE type_uri = 'http://ex.org/Customer' "
        "ORDER BY uri LIMIT 50 OFFSET 10"
    )


def test_entity_search_uri_search_sql_adds_text_clause_when_search_given() -> None:
    sql = entity_search_uri_search_sql(
        search_table="g_entity_search",
        type_uri="http://ex.org/Customer",
        search="Jac",
        limit=50,
        offset=0,
        escape=_escape,
    )
    assert "type_uri = 'http://ex.org/Customer'" in sql
    assert "(label_lc LIKE '%jac%' ESCAPE '!' OR uri_lc LIKE '%jac%' ESCAPE '!')" in sql
    assert " AND " in sql


def test_entity_search_seed_sql_matches_type_local_name_suffix() -> None:
    sql = entity_search_seed_sql(
        search_table="g_entity_search",
        entity_type="Customer",
        search="",
        escape=_escape,
    )
    assert sql == (
        "SELECT uri FROM g_entity_search WHERE "
        "(LOWER(type_uri) LIKE '%#customer' ESCAPE '!' OR LOWER(type_uri) LIKE '%/customer' ESCAPE '!')"
    )


def test_entity_search_seed_sql_combines_type_and_search() -> None:
    sql = entity_search_seed_sql(
        search_table="g_entity_search",
        entity_type="Customer",
        search="Jac",
        escape=_escape,
    )
    assert "(LOWER(type_uri) LIKE '%#customer' ESCAPE '!' OR LOWER(type_uri) LIKE '%/customer' ESCAPE '!')" in sql
    assert "(label_lc LIKE '%jac%' ESCAPE '!' OR uri_lc LIKE '%jac%' ESCAPE '!')" in sql
    assert " AND " in sql
    assert "LIMIT" not in sql
    assert "OFFSET" not in sql


def test_entity_search_seed_sql_search_only_has_no_type_clause() -> None:
    sql = entity_search_seed_sql(
        search_table="g_entity_search",
        entity_type="",
        search="ada",
        escape=_escape,
    )
    assert sql == (
        "SELECT uri FROM g_entity_search WHERE "
        "(label_lc LIKE '%ada%' ESCAPE '!' OR uri_lc LIKE '%ada%' ESCAPE '!')"
    )


def test_entity_search_seed_sql_escapes_entity_type() -> None:
    sql = entity_search_seed_sql(
        search_table="g_entity_search",
        entity_type="O'Brien",
        search="",
        escape=lambda s: s.replace("'", "''"),
    )
    assert "o''brien" in sql


def test_entity_search_uri_search_sql_no_type_filter() -> None:
    sql = entity_search_uri_search_sql(
        search_table="g_entity_search",
        type_uri="",
        search="",
        limit=10,
        escape=_escape,
    )
    assert sql == "SELECT uri FROM g_entity_search ORDER BY uri LIMIT 10 OFFSET 0"


def test_sort_preview_rows_orders_type_then_label_then_uri() -> None:
    rows = [
        {"uri": "u2", "type": "T", "label": "b"},
        {"uri": "u1", "type": "T", "label": "a"},
        {"uri": "u3", "type": "A", "label": "z"},
    ]
    assert [r["uri"] for r in sort_preview_rows(rows)] == ["u3", "u1", "u2"]
