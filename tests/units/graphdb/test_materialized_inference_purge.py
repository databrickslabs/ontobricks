"""Source-preserving purge contracts for generated graph companions."""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from back.core.graphdb.interfaces.GraphBackend import GraphBackend
from back.core.graphdb.delta.MaterializedStore import MaterializedStore
from back.core.graphdb.lakebase.FlatStore import FlatStore

pytestmark = pytest.mark.unit


def test_base_backend_reports_source_safe_purge_as_unsupported():
    assert GraphBackend.supports_materialized_inference_purge is False


def test_companion_backends_report_source_safe_purge_as_supported():
    assert MaterializedStore.supports_materialized_inference_purge is True
    assert FlatStore.supports_materialized_inference_purge is True


def test_base_backend_rejects_generated_purge():
    with pytest.raises(NotImplementedError, match="generated"):
        GraphBackend.purge_materialized_triples(MagicMock(), "sales_V3")


def test_delta_counts_and_clears_only_derived_overrides():
    client = MagicMock()
    store = MaterializedStore(client, domain=MagicMock(), settings=MagicMock())
    store.rebuild_adjacency = MagicMock()

    with (
        patch.object(
            store,
            "_writable_table_fqn",
            return_value="cat.sch.sales_overrides",
        ),
        patch.object(store, "get_inferred_triple_count", return_value=17) as count,
        patch(
            "back.core.graphdb.delta.MaterializedStore.materialize.clear_derived"
        ) as clear,
        patch("back.core.graphdb.search_cache.rebuild_graph_cache_if_enabled"),
    ):
        assert store.purge_materialized_triples("sales_V3") == 17

    count.assert_called_once_with("sales_V3", raise_errors=True)
    clear.assert_called_once_with(client, "cat.sch.sales_overrides")


def test_delta_purge_returns_zero_when_overrides_table_is_missing():
    client = MagicMock()
    store = MaterializedStore(client, domain=MagicMock(), settings=MagicMock())
    store.rebuild_adjacency = MagicMock()

    with (
        patch.object(
            store,
            "_writable_table_fqn",
            return_value="cat.sch.sales_overrides",
        ),
        patch.object(
            store,
            "get_inferred_triple_count",
            side_effect=RuntimeError("TABLE_OR_VIEW_NOT_FOUND: sales_overrides"),
        ),
        patch(
            "back.core.graphdb.delta.MaterializedStore.materialize.clear_derived"
        ) as clear,
    ):
        assert store.purge_materialized_triples("sales_V3") == 0

    clear.assert_not_called()
    store.rebuild_adjacency.assert_not_called()


def test_delta_purge_surfaces_adjacency_rebuild_failure():
    client = MagicMock()
    store = MaterializedStore(client, domain=MagicMock(), settings=MagicMock())
    store.rebuild_adjacency = MagicMock(side_effect=RuntimeError("adjacency failed"))

    with (
        patch.object(
            store,
            "_writable_table_fqn",
            return_value="cat.sch.sales_overrides",
        ),
        patch.object(store, "get_inferred_triple_count", return_value=17),
        patch(
            "back.core.graphdb.delta.MaterializedStore.materialize.clear_derived"
        ),
    ):
        with pytest.raises(RuntimeError, match="adjacency failed"):
            store.purge_materialized_triples("sales_V3")


def test_lakebase_counts_and_clears_only_derived_overrides():
    store = object.__new__(FlatStore)
    store._sync_mode = "app_managed"
    store.get_inferred_triple_count = MagicMock(return_value=9)
    store.rebuild_adjacency = MagicMock()
    cursor = MagicMock()
    cursor_context = MagicMock()
    cursor_context.__enter__.return_value = cursor
    cursor_context.__exit__.return_value = False
    store._cursor = MagicMock(return_value=cursor_context)

    with (
        patch.object(store, "companion_phy", return_value="g_sales_v3__overrides"),
        patch(
            "back.core.graphdb.lakebase.OverrideOpsMixin."
            "_companion_ddl.clear_derived"
        ) as truncate,
    ):
        assert store.purge_materialized_triples("sales_V3") == 9

    store.get_inferred_triple_count.assert_called_once_with("sales_V3")
    truncate.assert_called_once_with(cursor, "g_sales_v3__overrides")
    store.rebuild_adjacency.assert_called_once_with("sales_V3")


def test_lakebase_purge_skips_rebuild_when_search_cache_disabled():
    store = object.__new__(FlatStore)
    store._sync_mode = "app_managed"
    store._domain = SimpleNamespace(info={"graph_cache_enabled": False})
    store.get_inferred_triple_count = MagicMock(return_value=9)
    store.rebuild_adjacency = MagicMock()
    cursor = MagicMock()
    cursor_context = MagicMock()
    cursor_context.__enter__.return_value = cursor
    cursor_context.__exit__.return_value = False
    store._cursor = MagicMock(return_value=cursor_context)

    with (
        patch.object(store, "companion_phy", return_value="g_sales_v3__overrides"),
        patch(
            "back.core.graphdb.lakebase.OverrideOpsMixin."
            "_companion_ddl.clear_derived"
        ),
    ):
        assert store.purge_materialized_triples("sales_V3") == 9

    store.rebuild_adjacency.assert_not_called()


def test_lakebase_purge_surfaces_adjacency_rebuild_failure():
    store = object.__new__(FlatStore)
    store._sync_mode = "app_managed"
    store.get_inferred_triple_count = MagicMock(return_value=9)
    store.rebuild_adjacency = MagicMock(side_effect=RuntimeError("adjacency failed"))
    cursor = MagicMock()
    cursor_context = MagicMock()
    cursor_context.__enter__.return_value = cursor
    cursor_context.__exit__.return_value = False
    store._cursor = MagicMock(return_value=cursor_context)

    with (
        patch.object(store, "companion_phy", return_value="g_sales_v3__overrides"),
        patch(
            "back.core.graphdb.lakebase.OverrideOpsMixin."
            "_companion_ddl.clear_derived"
        ),
    ):
        with pytest.raises(RuntimeError, match="adjacency failed"):
            store.purge_materialized_triples("sales_V3")
