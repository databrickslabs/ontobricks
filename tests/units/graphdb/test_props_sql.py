"""SQL shape tests for the property companion projection."""

from unittest.mock import Mock

import pytest

from back.core.graphdb.constants import RDF_TYPE
from back.core.graphdb.props import (
    execute_expand_with_props_fallback,
    forget_missing_props,
    known_missing_props,
    props_select,
    remember_missing_props,
    reset_missing_props_cache,
)


def setup_function() -> None:
    reset_missing_props_cache()


def test_props_select_keeps_all_triples_for_typed_subjects():
    sql = props_select("g._graph")

    assert "SELECT t.subject, t.predicate, t.object" in sql
    assert sql.count("g._graph") == 2
    assert "INNER JOIN" in sql
    assert "typed.subject = t.subject" in sql
    assert f"predicate = '{RDF_TYPE}'" in sql
    assert "t.object LIKE 'http%'" not in sql
    assert "t.predicate !=" not in sql


def test_missing_props_cache_is_negative_only():
    assert known_missing_props("g_props") is False
    remember_missing_props("g_props")
    assert known_missing_props("g_props") is True
    forget_missing_props("g_props")
    assert known_missing_props("g_props") is False


def test_execute_skips_props_sql_when_table_is_known_missing():
    execute_query = Mock(return_value=[{"subject": "s"}])
    remember_missing_props("g_props")

    rows = execute_expand_with_props_fallback(
        execute_query=execute_query,
        sql="SELECT FROM g_props",
        props_table="g_props",
        fallback_sql="SELECT FROM g",
    )

    execute_query.assert_called_once_with("SELECT FROM g")
    assert rows == [{"subject": "s"}]


def test_execute_remembers_missing_props_and_retries_fallback():
    execute_query = Mock(
        side_effect=[
            RuntimeError("TABLE_OR_VIEW_NOT_FOUND: g_props"),
            [{"subject": "s"}],
        ]
    )

    rows = execute_expand_with_props_fallback(
        execute_query=execute_query,
        sql="SELECT FROM g_props",
        props_table="g_props",
        fallback_sql="SELECT FROM g",
    )

    assert known_missing_props("g_props") is True
    assert [call.args[0] for call in execute_query.call_args_list] == [
        "SELECT FROM g_props",
        "SELECT FROM g",
    ]
    assert rows == [{"subject": "s"}]


def test_execute_propagates_unrelated_failure():
    execute_query = Mock(side_effect=RuntimeError("permission denied"))

    with pytest.raises(RuntimeError, match="permission denied"):
        execute_expand_with_props_fallback(
            execute_query=execute_query,
            sql="SELECT FROM g_props",
            props_table="g_props",
            fallback_sql="SELECT FROM g",
        )

    assert known_missing_props("g_props") is False
