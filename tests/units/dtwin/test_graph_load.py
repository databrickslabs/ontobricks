"""Backend load of a source relation: streamed (GraphLoad) and Lakeflow (SyncedGraphLoad)."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

from back.core.errors import InfrastructureError
from back.objects.digitaltwin.build import GraphLoad, SyncedGraphLoad


def _load(cls=GraphLoad, **overrides):
    kwargs = dict(
        tm=MagicMock(),
        task_id="t-test",
        is_api=False,
        domain=SimpleNamespace(),
        settings=SimpleNamespace(),
        store=MagicMock(),
        source_client=MagicMock(),
        graph_name="G_V1",
        source_relation="cat.sch.v_g",
        delta_cfg={"catalog": "cat", "schema": "sch"},
        cancel_check=lambda: False,
    )
    kwargs.update(overrides)
    return cls(**kwargs)


class TestStreamTriplesIntoStore:
    def test_uses_bulk_insert_iter_when_available(self):
        load = _load()
        rows = [
            {"subject": "s1", "predicate": "p", "object": "o1"},
            {"subject": "s2", "predicate": "p", "object": "o2"},
        ]
        load.source_client.iter_rows.return_value = iter(rows)
        load.store.bulk_insert_iter = MagicMock(return_value=2)

        n = load.stream_triples_into_store(
            "SELECT subject, predicate, object FROM v", insert_batch_size=2000
        )

        assert n == 2
        load.source_client.iter_rows.assert_called_once_with(
            "SELECT subject, predicate, object FROM v", batch_size=2000
        )
        kwargs = load.store.bulk_insert_iter.call_args.kwargs
        assert kwargs["batch_size"] == 2000
        assert load.store.bulk_insert_iter.call_args.args[0] == "G_V1"
        load.store.insert_triples.assert_not_called()

    def test_falls_back_to_insert_triples_when_no_bulk_iter(self):
        store = SimpleNamespace(insert_triples=MagicMock(return_value=2))
        load = _load(store=store)
        rows = [
            {"subject": "s1", "predicate": "p", "object": "o1"},
            {"subject": "s2", "predicate": "p", "object": "o2"},
        ]
        load.source_client.iter_rows.return_value = iter(rows)

        n = load.stream_triples_into_store(
            "SELECT subject, predicate, object FROM v", insert_batch_size=5000
        )

        assert n == 2
        called_args, _ = store.insert_triples.call_args
        assert called_args[0] == "G_V1"
        assert called_args[1] == rows


class TestFullLoad:
    """Streamed full rebuild of the store from the source relation."""

    def _full_load(self, total: int, **overrides) -> GraphLoad:
        load = _load(**overrides)
        load.count_source_triples = MagicMock(return_value=total)
        load.captured = []
        load.tm.update_progress = MagicMock(
            side_effect=lambda tid, pct, msg: load.captured.append((pct, msg))
        )
        callbacks: dict = {}

        def _capture_bulk_load(table_name, triple_iter, batch_size=5000, on_progress=None):
            callbacks["fn"] = on_progress
            return 1

        load.store.bulk_load_into_sync = _capture_bulk_load
        load.source_client.iter_rows = MagicMock(return_value=iter([]))
        load.callbacks = callbacks
        return load

    def test_progress_message_uses_true_total_as_denominator(self):
        """Regression for the 'Written x/x' UI bug."""
        load = self._full_load(total=15000)

        assert load.run() == GraphLoad.LOADED

        cb = load.callbacks["fn"]
        cb(4500, 4500)
        cb(15000, 15000)
        msgs = [m for _pct, m in load.captured]
        assert any("Written 4500/15000 triples" in m for m in msgs), msgs
        assert any("Written 15000/15000 triples" in m for m in msgs), msgs
        assert not any("Written 4500/4500" in m for m in msgs), msgs

    def test_reads_triples_from_source_relation(self):
        load = self._full_load(total=10, source_relation="cat.sch.composed_relation")

        load.run()

        select_sql = load.source_client.iter_rows.call_args.args[0]
        assert select_sql.endswith("FROM cat.sch.composed_relation")

    def test_recreates_the_graph_before_loading(self):
        load = self._full_load(total=10)

        load.run()

        load.store.drop_table.assert_called_once_with("G_V1")
        load.store.create_table.assert_called_once_with("G_V1")

    def test_empty_source_writes_nothing(self):
        load = self._full_load(total=0)

        assert load.run() == GraphLoad.EMPTY

        load.store.drop_table.assert_not_called()
        load.source_client.iter_rows.assert_not_called()

    def test_rebuilds_adjacency_for_postgres_store(self):
        load = self._full_load(total=10)
        load.store.supports_adjacency = True
        load.store.sql_flavor.return_value = "postgres"
        load.store.rebuild_adjacency = MagicMock()
        load.store.optimize_table = MagicMock()

        assert load.run() == GraphLoad.LOADED

        load.store.optimize_table.assert_called_once_with("G_V1")
        load.store.rebuild_adjacency.assert_called_once_with("G_V1")

    def test_skips_adjacency_for_non_postgres_flavor(self):
        load = self._full_load(total=10)
        load.store.supports_adjacency = True
        load.store.sql_flavor.return_value = "spark"
        load.store.rebuild_adjacency = MagicMock()
        load.store.optimize_table = MagicMock()

        assert load.run() == GraphLoad.LOADED

        load.store.optimize_table.assert_called_once_with("G_V1")
        load.store.rebuild_adjacency.assert_not_called()


class TestSyncedLoad:
    """In synced mode, the app triggers Lakeflow and never iterates triples."""

    _SYNCED_UC = "cat.ontobricks_graph.g_v1_sync"

    def _synced_load(self, **overrides) -> SyncedGraphLoad:
        load = _load(SyncedGraphLoad, **overrides)
        store = load.store
        store.is_synced = True
        store.sync_table_mode = "snapshot"
        store.sync_timeout_s = 600
        store.sync_uc_catalog = ""
        store.graph_schema = "ontobricks_graph"
        store.synced_uc_name.return_value = self._SYNCED_UC
        mgr = MagicMock()
        # ensure() returns the requested name, so no ghost-state fallback.
        ensure_ret = MagicMock()
        ensure_ret.name = self._SYNCED_UC
        mgr.ensure.return_value = ensure_ret
        store.synced_manager.return_value = mgr
        store.ensure_synced_companion = MagicMock()
        store.ensure_synced_union_view = MagicMock()
        store.truncate_companion = MagicMock()
        load.count_source_triples = MagicMock(return_value=1234)
        load.mgr = mgr
        return load

    def test_a_failed_post_sync_count_does_not_fail_the_sync(self):
        load = self._synced_load()
        load.count_source_triples = MagicMock(
            side_effect=InfrastructureError(
                "Could not count the triples in cat.sch.v_g: "
                "Error during request to server"
            )
        )

        assert load.run() == GraphLoad.LOADED
        assert any(
            "count unavailable" in str(call)
            for call in load.tm.update_progress.call_args_list
        )

    def test_calls_ensure_trigger_and_truncate_companion(self):
        load = self._synced_load()

        assert load.run() == GraphLoad.LOADED

        ensure_kwargs = load.mgr.ensure.call_args.kwargs
        assert ensure_kwargs["source_table_full_name"] == "cat.sch.v_g"
        assert ensure_kwargs["primary_key_columns"] == [
            "subject",
            "predicate",
            "object_hash",
        ]
        assert ensure_kwargs["sync_mode"] == "snapshot"
        load.store.ensure_synced_companion.assert_called_once_with("G_V1")
        load.store.ensure_synced_union_view.assert_called_once_with(
            "G_V1", synced_phy_override=None
        )
        load.mgr.trigger_and_wait.assert_called_once()
        load.store.truncate_companion.assert_called_once_with("G_V1")
        load.store.bulk_insert_iter.assert_not_called()
        load.store.bulk_delete_iter.assert_not_called()
        load.source_client.iter_rows.assert_not_called()
        assert load.triple_count == 1234

    def test_synced_table_is_registered_on_source_relation(self):
        load = self._synced_load(source_relation="cat.sch.composed_relation")

        load.run()

        ensure_kwargs = load.mgr.ensure.call_args.kwargs
        assert ensure_kwargs["source_table_full_name"] == "cat.sch.composed_relation"

    def test_fallback_registration_name_drives_the_union_view(self):
        load = self._synced_load()
        load.mgr.ensure.return_value.name = "cat.ontobricks_graph.g_v1_sync_2"

        assert load.run() == GraphLoad.LOADED

        load.store.ensure_synced_union_view.assert_called_once_with(
            "G_V1", synced_phy_override="g_v1_sync_2"
        )

    def test_rebuilds_adjacency_for_postgres_store(self):
        load = self._synced_load()
        load.store.supports_adjacency = True
        load.store.sql_flavor.return_value = "postgres"
        load.store.rebuild_adjacency = MagicMock()

        assert load.run() == GraphLoad.LOADED

        load.store.rebuild_adjacency.assert_called_once_with("G_V1")

    def test_fails_when_adjacency_rebuild_fails(self):
        load = self._synced_load()
        load.store.supports_adjacency = True
        load.store.sql_flavor.return_value = "postgres"
        load.store.rebuild_adjacency.side_effect = RuntimeError("adj stale")

        assert load.run() == GraphLoad.FAILED

        load.store.truncate_companion.assert_called_once_with("G_V1")
        load.tm.fail_task.assert_called_once()
        assert "adjacency" in load.tm.fail_task.call_args.args[1].lower()

    def test_attempts_adjacency_even_when_truncate_fails(self):
        load = self._synced_load()
        load.store.supports_adjacency = True
        load.store.sql_flavor.return_value = "postgres"
        load.store.truncate_companion.side_effect = RuntimeError("truncate failed")
        load.store.rebuild_adjacency = MagicMock()

        assert load.run() == GraphLoad.LOADED

        load.store.rebuild_adjacency.assert_called_once_with("G_V1")
