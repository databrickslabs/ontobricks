"""Source-preserving purge contracts for generated graph companions."""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from back.core.graphdb.GraphDBBackend import GraphDBBackend
from back.core.graphdb.delta.DeltaFlatStore import DeltaFlatStore
from back.core.graphdb.lakebase.LakebaseFlatStore import LakebaseFlatStore

pytestmark = pytest.mark.unit


def test_base_backend_reports_source_safe_purge_as_unsupported():
    assert GraphDBBackend.supports_materialized_inference_purge is False


def test_companion_backends_report_source_safe_purge_as_supported():
    assert DeltaFlatStore.supports_materialized_inference_purge is True
    assert LakebaseFlatStore.supports_materialized_inference_purge is True


def test_base_backend_rejects_generated_purge():
    with pytest.raises(NotImplementedError, match="generated"):
        GraphDBBackend.purge_materialized_triples(MagicMock(), "sales_V3")


def test_delta_counts_and_truncates_only_inferred_companion():
    client = MagicMock()
    store = DeltaFlatStore(client, domain=MagicMock(), settings=MagicMock())
    store.rebuild_adjacency = MagicMock()

    with (
        patch.object(
            store,
            "_writable_table_fqn",
            return_value="cat.sch.sales_inferred",
        ),
        patch.object(store, "count_triples", return_value=17) as count,
        patch(
            "back.core.graphdb.delta.DeltaFlatStore.materialize.truncate_table"
        ) as truncate,
    ):
        assert store.purge_materialized_triples("sales_V3") == 17

    count.assert_called_once_with("cat.sch.sales_inferred")
    truncate.assert_called_once_with(client, "cat.sch.sales_inferred")
    store.rebuild_adjacency.assert_called_once_with("sales_V3")


def test_delta_purge_returns_zero_when_inferred_companion_is_missing():
    client = MagicMock()
    store = DeltaFlatStore(client, domain=MagicMock(), settings=MagicMock())
    store.rebuild_adjacency = MagicMock()

    with (
        patch.object(
            store,
            "_writable_table_fqn",
            return_value="cat.sch.sales_inferred",
        ),
        patch.object(
            store,
            "count_triples",
            side_effect=RuntimeError("TABLE_OR_VIEW_NOT_FOUND: sales_inferred"),
        ),
        patch(
            "back.core.graphdb.delta.DeltaFlatStore.materialize.truncate_table"
        ) as truncate,
    ):
        assert store.purge_materialized_triples("sales_V3") == 0

    truncate.assert_not_called()
    store.rebuild_adjacency.assert_not_called()


def test_delta_purge_surfaces_adjacency_rebuild_failure():
    client = MagicMock()
    store = DeltaFlatStore(client, domain=MagicMock(), settings=MagicMock())
    store.rebuild_adjacency = MagicMock(side_effect=RuntimeError("adjacency failed"))

    with (
        patch.object(
            store,
            "_writable_table_fqn",
            return_value="cat.sch.sales_inferred",
        ),
        patch.object(store, "count_triples", return_value=17),
        patch(
            "back.core.graphdb.delta.DeltaFlatStore.materialize.truncate_table"
        ),
    ):
        with pytest.raises(RuntimeError, match="adjacency failed"):
            store.purge_materialized_triples("sales_V3")


def test_lakebase_counts_and_truncates_only_app_companion():
    store = object.__new__(LakebaseFlatStore)
    store._sync_mode = "app_managed"
    store.count_triples = MagicMock(return_value=9)
    store.rebuild_adjacency = MagicMock()
    cursor = MagicMock()
    cursor_context = MagicMock()
    cursor_context.__enter__.return_value = cursor
    cursor_context.__exit__.return_value = False
    store._cursor = MagicMock(return_value=cursor_context)

    with (
        patch.object(store, "companion_phy", return_value="g_sales_v3__app"),
        patch(
            "back.core.graphdb.lakebase.LakebaseFlatStore."
            "_companion_ddl.truncate_companion"
        ) as truncate,
    ):
        assert store.purge_materialized_triples("sales_V3") == 9

    store.count_triples.assert_called_once_with("g_sales_v3__app")
    truncate.assert_called_once_with(cursor, "g_sales_v3__app")
    store.rebuild_adjacency.assert_called_once_with("sales_V3")


def test_lakebase_purge_skips_rebuild_when_search_cache_disabled():
    store = object.__new__(LakebaseFlatStore)
    store._sync_mode = "app_managed"
    store._domain = SimpleNamespace(info={"graph_cache_enabled": False})
    store.count_triples = MagicMock(return_value=9)
    store.rebuild_adjacency = MagicMock()
    cursor = MagicMock()
    cursor_context = MagicMock()
    cursor_context.__enter__.return_value = cursor
    cursor_context.__exit__.return_value = False
    store._cursor = MagicMock(return_value=cursor_context)

    with (
        patch.object(store, "companion_phy", return_value="g_sales_v3__app"),
        patch(
            "back.core.graphdb.lakebase.LakebaseFlatStore."
            "_companion_ddl.truncate_companion"
        ),
    ):
        assert store.purge_materialized_triples("sales_V3") == 9

    store.rebuild_adjacency.assert_not_called()


def test_lakebase_purge_surfaces_adjacency_rebuild_failure():
    store = object.__new__(LakebaseFlatStore)
    store._sync_mode = "app_managed"
    store.count_triples = MagicMock(return_value=9)
    store.rebuild_adjacency = MagicMock(side_effect=RuntimeError("adjacency failed"))
    cursor = MagicMock()
    cursor_context = MagicMock()
    cursor_context.__enter__.return_value = cursor
    cursor_context.__exit__.return_value = False
    store._cursor = MagicMock(return_value=cursor_context)

    with (
        patch.object(store, "companion_phy", return_value="g_sales_v3__app"),
        patch(
            "back.core.graphdb.lakebase.LakebaseFlatStore."
            "_companion_ddl.truncate_companion"
        ),
    ):
        with pytest.raises(RuntimeError, match="adjacency failed"):
            store.purge_materialized_triples("sales_V3")
