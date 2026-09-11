"""Source-preserving purge contracts for generated graph companions."""

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


def test_lakebase_counts_and_truncates_only_app_companion():
    store = object.__new__(LakebaseFlatStore)
    store._sync_mode = "app_managed"
    store.count_triples = MagicMock(return_value=9)
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
