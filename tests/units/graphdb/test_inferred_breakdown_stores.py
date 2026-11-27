"""Store wrappers: resolve the relation, build the SQL, run it, normalise."""

from contextlib import contextmanager
from unittest.mock import MagicMock, patch

from back.core.graphdb.delta import materialize
from back.core.graphdb.delta.MaterializedStore import MaterializedStore
from back.core.graphdb.interfaces.GraphBackend import GraphBackend
from back.core.graphdb.lakebase import _companion_ddl
from back.core.graphdb.lakebase.FlatStore import FlatStore

BASE = "http://ontobricks.io/ns/demo#"


@contextmanager
def _cursor_cm(cur):
    """Stand-in for ``FlatStore._cursor()``: a context manager yielding *cur*."""
    yield cur


def test_graph_backend_default_is_unsupported():
    assert GraphBackend.supports_inferred_breakdown is False


def test_flat_store_flag_on():
    assert FlatStore.supports_inferred_breakdown is True


def test_materialized_store_flag_on():
    assert MaterializedStore.supports_inferred_breakdown is True


def test_materialized_store_without_domain_returns_zero_rows():
    store = MaterializedStore(MagicMock())
    with patch.object(store, "execute_query") as run:
        result = store.get_inferred_breakdown("dom", BASE)

    run.assert_not_called()
    assert [r["triples"] for r in result] == [0, 0, 0]


def test_materialized_store_runs_breakdown_on_writable_table():
    store = MaterializedStore(MagicMock())
    rows = [{"category": "cohorts", "triples": 5, "entities": 3, "groups": 1}]
    with (
        patch.object(store, "_domain", new=MagicMock()),
        patch.object(store, "_writable_table_fqn", return_value="cat.sch.dom_inferred") as fqn,
        patch.object(materialize, "relation_exists", return_value=True),
        patch.object(store, "execute_query", return_value=rows) as run,
    ):
        result = store.get_inferred_breakdown("dom", BASE)

    fqn.assert_called_once_with("dom")
    sql = run.call_args.args[0]
    assert "FROM cat.sch.dom_inferred" in sql
    cohorts = next(r for r in result if r["category"] == "cohorts")
    assert (cohorts["triples"], cohorts["entities"], cohorts["groups"]) == (5, 3, 1)
    assert next(r for r in result if r["category"] == "reasoning")["triples"] == 0


def test_flat_store_runs_breakdown_on_companion_table():
    store = FlatStore.__new__(FlatStore)
    with (
        patch.object(store, "execute_query", return_value=[]) as run,
        patch.object(store, "_readable_table_id", return_value='"s"."dom__overrides"') as rel,
        patch.object(store, "_cursor", return_value=_cursor_cm(MagicMock())),
        patch.object(_companion_ddl, "relation_exists", return_value=True),
    ):
        result = store.get_inferred_breakdown("dom", BASE)

    rel.assert_called_once()
    assert rel.call_args.args[0].endswith("__overrides")
    sql = run.call_args.args[0]
    assert '"s"."dom__overrides"' in sql
    assert [r["category"] for r in result] == ["reasoning", "cohorts", "communities"]


def test_flat_store_returns_zero_rows_when_overrides_table_missing():
    store = FlatStore.__new__(FlatStore)
    with (
        patch.object(store, "execute_query", return_value=[]) as run,
        patch.object(store, "_readable_table_id", return_value="dom__overrides"),
        patch.object(store, "_cursor", return_value=_cursor_cm(MagicMock())),
        patch.object(_companion_ddl, "relation_exists", return_value=False),
    ):
        result = store.get_inferred_breakdown("dom", BASE)

    run.assert_not_called()
    assert [r["triples"] for r in result] == [0, 0, 0]


def test_materialized_store_returns_zero_rows_when_overrides_table_missing():
    store = MaterializedStore(MagicMock())
    with (
        patch.object(store, "_domain", new=MagicMock()),
        patch.object(store, "_writable_table_fqn", return_value="cat.sch.dom_overrides"),
        patch.object(materialize, "relation_exists", return_value=False) as exists,
        patch.object(store, "execute_query") as run,
    ):
        result = store.get_inferred_breakdown("dom", BASE)

    exists.assert_called_once()
    assert exists.call_args.args[1] == "cat.sch.dom_overrides"
    run.assert_not_called()
    assert [r["triples"] for r in result] == [0, 0, 0]
