"""Streaming + Lakebase-aware behaviour for the Digital Twin build pipeline.

These tests exercise the helpers in :class:`_BuildPipeline` without spinning
up the whole pipeline. Each test builds a minimal instance via
``object.__new__`` and only sets the attributes the helper needs, which keeps
the test scope tight and fast.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from back.core.triplestore.TripleStoreFactory import TripleStoreFactory
from back.objects.digitaltwin._build_pipeline import _BuildPipeline


def _bare_pipeline(**overrides):
    """Return a ``_BuildPipeline`` instance with only the attributes we patch."""
    pipe = object.__new__(_BuildPipeline)
    pipe.task_id = "t-test"
    pipe.graph_name = "G_V1"
    pipe.view_table = "cat.sch.v_g"
    pipe.source_client = MagicMock()
    pipe.store = MagicMock()
    pipe.tm = MagicMock()
    pipe.is_api = False
    pipe.archive_task_id = None
    pipe.phase_times = {}
    pipe.domain = SimpleNamespace()
    pipe.settings = SimpleNamespace()
    pipe.delta_cfg = {"catalog": "cat", "schema": "sch"}
    pipe._lakebase_engine_config = {}
    pipe._is_lakebase_synced = False
    pipe.triple_count = 0
    for k, v in overrides.items():
        setattr(pipe, k, v)
    return pipe


class TestStreamTriplesIntoStore:
    def test_uses_bulk_insert_iter_when_available(self):
        pipe = _bare_pipeline()
        rows = [
            {"subject": "s1", "predicate": "p", "object": "o1"},
            {"subject": "s2", "predicate": "p", "object": "o2"},
        ]
        pipe.source_client.iter_rows.return_value = iter(rows)
        pipe.store.bulk_insert_iter = MagicMock(return_value=2)

        n = pipe._stream_triples_into_store(
            "SELECT subject, predicate, object FROM v",
            insert_batch_size=2000,
        )

        assert n == 2
        pipe.source_client.iter_rows.assert_called_once_with(
            "SELECT subject, predicate, object FROM v", batch_size=2000
        )
        pipe.store.bulk_insert_iter.assert_called_once()
        kwargs = pipe.store.bulk_insert_iter.call_args.kwargs
        assert kwargs["batch_size"] == 2000
        assert pipe.store.bulk_insert_iter.call_args.args[0] == "G_V1"
        pipe.store.insert_triples.assert_not_called()

    def test_falls_back_to_insert_triples_when_no_bulk_iter(self):
        pipe = _bare_pipeline()
        store = SimpleNamespace(insert_triples=MagicMock(return_value=2))
        pipe.store = store
        rows = [
            {"subject": "s1", "predicate": "p", "object": "o1"},
            {"subject": "s2", "predicate": "p", "object": "o2"},
        ]
        pipe.source_client.iter_rows.return_value = iter(rows)

        n = pipe._stream_triples_into_store(
            "SELECT subject, predicate, object FROM v",
            insert_batch_size=5000,
        )
        assert n == 2
        called_args, called_kwargs = store.insert_triples.call_args
        assert called_args[0] == "G_V1"
        assert called_args[1] == rows


class TestFullRebuildProgressMessage:
    """Regression for the 'Written x/x' UI bug.

    ``bulk_insert_iter`` does not know the upfront target and passes the
    running written count as ``total``; the pipeline must anchor the
    denominator on the warehouse-side COUNT(*) total so the UI reads
    ``Written x/y`` (true total), not ``Written x/x``.
    """

    def _full_pipe(self, view_total: int):
        pipe = _bare_pipeline(is_api=False)
        pipe.start_time = 0.0
        # Mock the server-side count.
        pipe._count_view_triples = MagicMock(return_value=view_total)
        pipe._captured: list = []
        pipe.tm.update_progress = MagicMock(
            side_effect=lambda tid, pct, msg: pipe._captured.append((pct, msg))
        )
        captured_cb: dict = {}

        # _apply_full_rebuild now routes through bulk_load_into_sync (Lakebase
        # store exposes it via MagicMock auto-creation). Capture the callback
        # from the new code path instead of the legacy _stream_triples_into_store.
        def _capture_bulk_load(table_name, triple_iter, batch_size=5000, on_progress=None):
            captured_cb["fn"] = on_progress
            return 1  # pretend 1 row written

        pipe.store.bulk_load_into_sync = _capture_bulk_load
        pipe.source_client.iter_rows = MagicMock(return_value=iter([]))
        pipe._captured_cb = captured_cb
        return pipe

    def test_full_progress_message_uses_true_total_as_denominator(self):
        pipe = self._full_pipe(view_total=15000)

        ok = pipe._apply_full_rebuild()

        assert ok is True
        cb = pipe._captured_cb["fn"]
        assert callable(cb)
        cb(4500, 4500)
        cb(15000, 15000)

        msgs = [m for _pct, m in pipe._captured]
        assert any("Written 4500/15000 triples" in m for m in msgs), msgs
        assert any("Written 15000/15000 triples" in m for m in msgs), msgs
        assert not any("Written 4500/4500" in m for m in msgs), msgs


# ---------------------------------------------------------------
# Lakebase managed-synced apply path
# ---------------------------------------------------------------


class TestApplyViaSyncedPipeline:
    """In synced mode, the app must trigger Lakeflow and never iterate triples."""

    _SYNCED_UC = "cat.ontobricks_graph.g_v1_sync"

    def _synced_pipe(self) -> _BuildPipeline:
        pipe = _bare_pipeline(_is_lakebase_synced=True)
        pipe.store.is_synced = True
        pipe.store.sync_table_mode = "snapshot"
        pipe.store.sync_timeout_s = 600
        pipe.store.sync_uc_catalog = ""
        pipe.store.graph_schema = "ontobricks_graph"
        pipe.store.synced_uc_name.return_value = self._SYNCED_UC
        mgr = MagicMock()
        # ensure() must return an object whose .name matches synced_uc so the
        # build pipeline doesn't treat it as a ghost-state fallback.
        _ensure_ret = MagicMock()
        _ensure_ret.name = self._SYNCED_UC
        mgr.ensure.return_value = _ensure_ret
        pipe.store.synced_manager.return_value = mgr
        pipe.store.ensure_synced_companion = MagicMock()
        pipe.store.ensure_synced_union_view = MagicMock()
        pipe.store.truncate_companion = MagicMock()
        pipe._count_view_triples = MagicMock(return_value=1234)
        pipe._mgr = mgr
        # _raise_if_cancelled calls tm.is_cancelled(); return False so cancel
        # checks don't abort the pipeline in unit tests.
        pipe.tm.is_cancelled.return_value = False
        return pipe

    def test_synced_calls_ensure_trigger_and_truncate_companion(self):
        pipe = self._synced_pipe()

        ok = pipe._apply_via_synced_pipeline()

        assert ok is True
        pipe._mgr.ensure.assert_called_once()
        ensure_kwargs = pipe._mgr.ensure.call_args.kwargs
        assert ensure_kwargs["source_table_full_name"] == pipe.view_table
        assert ensure_kwargs["primary_key_columns"] == [
            "subject",
            "predicate",
            "object",
        ]
        assert ensure_kwargs["sync_mode"] == "snapshot"
        pipe.store.ensure_synced_companion.assert_called_once_with(pipe.graph_name)
        # synced_phy_override=None because actual name == requested name (no fallback).
        pipe.store.ensure_synced_union_view.assert_called_once_with(
            pipe.graph_name, synced_phy_override=None
        )
        pipe._mgr.trigger_and_wait.assert_called_once()
        pipe.store.truncate_companion.assert_called_once_with(pipe.graph_name)
        # CRITICAL: app never iterates triples in synced mode.
        pipe.store.bulk_insert_iter.assert_not_called()
        pipe.store.bulk_delete_iter.assert_not_called()
        pipe.source_client.iter_rows.assert_not_called()
        assert pipe.triple_count == 1234

    def test_apply_full_rebuild_branches_to_synced_path(self):
        pipe = self._synced_pipe()

        with patch.object(pipe, "_apply_via_synced_pipeline", return_value=True) as m:
            ok = pipe._apply_full_rebuild()

        assert ok is True
        m.assert_called_once_with()
        pipe.source_client.iter_rows.assert_not_called()


class TestResolveLakebaseMode:
    """Mode resolution must run before the store is opened."""

    def test_sets_synced_flag_when_global_config_says_so(self):
        pipe = _bare_pipeline()

        with (
            patch.object(TripleStoreFactory, "_resolve_graph_engine", return_value="lakebase"),
            patch.object(TripleStoreFactory, "_resolve_graph_engine_config", return_value={"sync_mode": "managed_synced"}),
        ):
            pipe._resolve_lakebase_mode()

        assert pipe._is_lakebase_synced is True
        assert pipe._lakebase_managed_synced() is True

    def test_default_is_app_managed(self):
        pipe = _bare_pipeline()

        with (
            patch.object(TripleStoreFactory, "_resolve_graph_engine", return_value="lakebase"),
            patch.object(TripleStoreFactory, "_resolve_graph_engine_config", return_value={}),
        ):
            pipe._resolve_lakebase_mode()

        assert pipe._is_lakebase_synced is False

    def test_unknown_engine_is_never_synced(self):
        """Future engines other than Lakebase do not enable synced mode."""
        pipe = _bare_pipeline()

        with (
            patch.object(TripleStoreFactory, "_resolve_graph_engine", return_value="other"),
            patch.object(TripleStoreFactory, "_resolve_graph_engine_config", return_value={"sync_mode": "managed_synced"}),
        ):
            pipe._resolve_lakebase_mode()

        assert pipe._is_lakebase_synced is False
