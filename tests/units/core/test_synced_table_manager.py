"""Unit tests for :class:`SyncedTableManager`.

Covers idempotent ``ensure``, refresh trigger, polling, deletion, and the
SDK-shape extraction helpers (``pipeline_id`` / state). The Databricks SDK
is mocked end-to-end so tests run without network or auth.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from back.core.errors import (
    InfrastructureError,
    OperationCancelledError,
    ValidationError,
)
from back.core.graphdb.lakebase.SyncedTableManager import SyncedTableManager
from databricks.sdk.service.pipelines import StartUpdateCause


def mock_method(obj: Any, name: str, **kw):
    """Patch an instance method so the test does not need the real Databricks SDK."""
    return patch.object(obj, name, MagicMock(**kw))


def _make_manager(client: MagicMock, sleeps: list | None = None) -> SyncedTableManager:
    """Build a SyncedTableManager with the SDK + sleep wired to mocks."""
    if sleeps is None:
        sleeps = []
    return SyncedTableManager(
        project_name="proj-123",
        branch_name="demo",
        client_factory=lambda: client,
        sleep=lambda s: sleeps.append(s),
    )


def _synced(state: str = "ONLINE_NO_PENDING_UPDATE", pipeline_id: str = "pid-1") -> Any:
    return SimpleNamespace(
        data_synchronization_status=SimpleNamespace(
            detailed_state=state,
            pipeline_id=pipeline_id,
        )
    )


# ---------------------------------------------------------------
# get / exists
# ---------------------------------------------------------------


class TestGetExists:
    def test_get_returns_synced_table(self):
        client = MagicMock()
        client.database.get_synced_database_table.return_value = _synced()
        mgr = _make_manager(client)
        out = mgr.get("cat.sch.tab")
        assert out is not None
        client.database.get_synced_database_table.assert_called_once_with(
            name="cat.sch.tab"
        )

    def test_get_returns_none_on_not_found(self):
        client = MagicMock()
        client.database.get_synced_database_table.side_effect = RuntimeError(
            "Resource not found: cat.sch.tab"
        )
        mgr = _make_manager(client)
        assert mgr.get("cat.sch.tab") is None

    def test_get_reraises_other_errors(self):
        client = MagicMock()
        client.database.get_synced_database_table.side_effect = RuntimeError(
            "internal server error"
        )
        mgr = _make_manager(client)
        with pytest.raises(RuntimeError):
            mgr.get("cat.sch.tab")

    def test_exists_true_when_get_returns_object(self):
        client = MagicMock()
        client.database.get_synced_database_table.return_value = _synced()
        mgr = _make_manager(client)
        assert mgr.exists("cat.sch.tab") is True

    def test_exists_false_when_not_found(self):
        client = MagicMock()
        client.database.get_synced_database_table.side_effect = RuntimeError(
            "404 not found"
        )
        mgr = _make_manager(client)
        assert mgr.exists("cat.sch.tab") is False


# ---------------------------------------------------------------
# ensure -- idempotent create
# ---------------------------------------------------------------


class TestEnsure:
    def test_ensure_returns_existing_without_creating(self):
        client = MagicMock()
        client.database.get_synced_database_table.return_value = _synced()
        mgr = _make_manager(client)
        out = mgr.ensure(
            "cat.sch.tab",
            source_table_full_name="cat.sch.view",
            primary_key_columns=["subject", "predicate", "object"],
        )
        assert out is not None
        client.database.create_synced_database_table.assert_not_called()

    def test_ensure_creates_when_missing(self):
        client = MagicMock()
        # First call: table not found (existence check before create).
        # Second call onwards: table is visible (post-create visibility poll).
        client.database.get_synced_database_table.side_effect = [None, _synced()]
        client.database.create_synced_database_table.return_value = _synced()

        mgr = _make_manager(client)
        # Avoid pulling the live ``databricks-sdk`` module by stubbing the
        # payload builder directly.
        captured: dict = {}

        def _fake_payload(**kw):
            captured["payload"] = kw
            return SimpleNamespace(**kw)

        with mock_method(mgr, "_build_synced_table_payload", side_effect=_fake_payload):
            mgr.ensure(
                "cat.sch.tab",
                source_table_full_name="cat.sch.view",
                primary_key_columns=["subject", "predicate", "object"],
                sync_mode="snapshot",
            )
        client.database.create_synced_database_table.assert_called_once()
        assert captured["payload"]["name"] == "cat.sch.tab"
        assert captured["payload"]["source_table_full_name"] == "cat.sch.view"
        assert captured["payload"]["primary_key_columns"] == [
            "subject",
            "predicate",
            "object",
        ]
        assert captured["payload"]["sync_mode"] == "snapshot"
        # project/branch are instance attrs consumed by the builder internals.
        assert mgr._project == "proj-123"
        assert mgr._branch == "demo"

    def test_ensure_swallows_already_exists_race(self):
        client = MagicMock()
        # Initial GET → None; POST → 409 "already exists"; ownership probe
        # GET → returns the table on first probe attempt (table became visible
        # before the probe loop times out, so we return early without delete).
        client.database.get_synced_database_table.side_effect = [
            None,      # initial ensure() check
            _synced(), # first ownership-probe GET → found → early return
        ]
        client.database.create_synced_database_table.side_effect = RuntimeError(
            "ALREADY_EXISTS: cat.sch.tab"
        )

        mgr = _make_manager(client)
        # Patch _sleep so the probe loop doesn't actually wait 30 s.
        with mock_method(mgr, "_sleep"), mock_method(
            mgr, "_build_synced_table_payload", return_value=SimpleNamespace()
        ):
            out = mgr.ensure(
                "cat.sch.tab",
                source_table_full_name="cat.sch.view",
                primary_key_columns=["subject", "predicate", "object"],
            )
        assert out is not None  # ownership-probe returned the winner


# ---------------------------------------------------------------
# trigger_refresh / wait_for_completion
# ---------------------------------------------------------------


class TestRefreshAndWait:
    def test_trigger_refresh_calls_pipelines_start_update(self):
        client = MagicMock()
        client.database.get_synced_database_table.return_value = _synced(
            pipeline_id="pid-42"
        )
        update = SimpleNamespace(update_id="run-1")
        client.pipelines.start_update.return_value = update
        mgr = _make_manager(client)
        run_id = mgr.trigger_refresh("cat.sch.tab")
        client.pipelines.start_update.assert_called_once_with(
            pipeline_id="pid-42",
            full_refresh=True,
            cause=StartUpdateCause.API_CALL,
        )
        assert run_id == "run-1"

    def test_trigger_refresh_raises_when_synced_missing(self):
        client = MagicMock()
        client.database.get_synced_database_table.return_value = None
        mgr = _make_manager(client)
        with pytest.raises(ValidationError):
            mgr.trigger_refresh("cat.sch.tab")

    def test_trigger_refresh_raises_when_pipeline_id_missing(self):
        client = MagicMock()
        client.database.get_synced_database_table.return_value = _synced(
            pipeline_id=""
        )
        mgr = _make_manager(client)
        with pytest.raises(InfrastructureError):
            mgr.trigger_refresh("cat.sch.tab")

    def test_trigger_refresh_skips_when_pipeline_already_has_active_update(self):
        """Lakeflow rejects a second start_update while one is running — poll instead."""
        client = MagicMock()
        client.database.get_synced_database_table.return_value = _synced(
            pipeline_id="pid-42"
        )
        client.pipelines.start_update.side_effect = RuntimeError(
            "An active update 'run-1' already exists for pipeline 'pid-42'."
        )
        mgr = _make_manager(client)
        assert mgr.trigger_refresh("cat.sch.tab") == ""
        client.pipelines.start_update.assert_called_once_with(
            pipeline_id="pid-42",
            full_refresh=True,
            cause=StartUpdateCause.API_CALL,
        )

    def test_wait_for_completion_returns_on_terminal_ok(self):
        client = MagicMock()
        client.database.get_synced_database_table.side_effect = [
            _synced(state="PROVISIONING"),
            _synced(state="ONLINE_TRIGGERED_UPDATE"),
            _synced(state="ONLINE"),
        ]
        sleeps: list = []
        mgr = _make_manager(client, sleeps=sleeps)
        state = mgr.wait_for_completion("cat.sch.tab", timeout_s=60)
        assert state == "ONLINE"
        # One sleep after Lakeflow idle wait, while state is not yet terminal.
        assert len(sleeps) == 1

    def test_wait_for_completion_calls_pipeline_idle_wait_when_pipeline_id_known(self):
        client = MagicMock()
        client.database.get_synced_database_table.return_value = _synced(
            state="ONLINE_NO_PENDING_UPDATE"
        )
        mgr = _make_manager(client)
        state = mgr.wait_for_completion("cat.sch.tab", timeout_s=120)
        assert state == "ONLINE_NO_PENDING_UPDATE"
        client.pipelines.wait_get_pipeline_idle.assert_called_once()
        assert (
            client.pipelines.wait_get_pipeline_idle.call_args.kwargs["pipeline_id"]
            == "pid-1"
        )

    def test_wait_for_completion_tracks_get_update_when_pipeline_update_id_set(self):
        """After start_update, wait on that update id — not a stale idle pipeline."""
        client = MagicMock()
        client.database.get_synced_database_table.return_value = _synced(
            state="ONLINE_NO_PENDING_UPDATE"
        )
        done = SimpleNamespace(
            update=SimpleNamespace(state=SimpleNamespace(name="COMPLETED"))
        )
        client.pipelines.get_update.return_value = done
        mgr = _make_manager(client)
        out = mgr.wait_for_completion(
            "cat.sch.tab",
            timeout_s=60,
            pipeline_update_id="upd-99",
        )
        assert out == "ONLINE_NO_PENDING_UPDATE"
        client.pipelines.get_update.assert_called()
        client.pipelines.wait_get_pipeline_idle.assert_not_called()

    def test_wait_for_completion_raises_on_terminal_failure(self):
        client = MagicMock()
        client.database.get_synced_database_table.return_value = _synced(
            state="FAILED"
        )
        mgr = _make_manager(client)
        with pytest.raises(InfrastructureError, match="FAILED"):
            mgr.wait_for_completion("cat.sch.tab", timeout_s=10)

    def test_wait_for_completion_aborts_when_cancel_check_returns_true(self):
        """Cancel signal between polls must surface as OperationCancelledError."""
        client = MagicMock()
        client.database.get_synced_database_table.return_value = _synced(
            state="PROVISIONING"
        )
        mgr = _make_manager(client)
        with pytest.raises(OperationCancelledError, match="Cancelled"):
            mgr.wait_for_completion(
                "cat.sch.tab",
                timeout_s=60,
                cancel_check=lambda: True,
            )

    def test_wait_for_completion_with_update_id_aborts_on_cancel(self):
        """Cancellation while polling a tracked pipeline update must abort."""
        client = MagicMock()
        client.database.get_synced_database_table.return_value = _synced(
            state="ONLINE_NO_PENDING_UPDATE"
        )
        # get_update keeps reporting RUNNING — the cancel check is the only escape.
        running = SimpleNamespace(
            update=SimpleNamespace(state=SimpleNamespace(name="RUNNING"))
        )
        client.pipelines.get_update.return_value = running
        mgr = _make_manager(client)
        with pytest.raises(OperationCancelledError):
            mgr.wait_for_completion(
                "cat.sch.tab",
                timeout_s=60,
                pipeline_update_id="upd-1",
                cancel_check=lambda: True,
            )

    def test_wait_for_completion_swallows_cancel_check_exceptions(self):
        """A broken cancel_check must not poison the wait loop."""
        client = MagicMock()
        client.database.get_synced_database_table.return_value = _synced(
            state="ONLINE"
        )
        mgr = _make_manager(client)

        def bad_check():
            raise RuntimeError("downstream blew up")

        state = mgr.wait_for_completion(
            "cat.sch.tab", timeout_s=60, cancel_check=bad_check
        )
        assert state == "ONLINE"

    def test_wait_for_completion_raises_on_timeout(self, monkeypatch):
        from itertools import chain, repeat

        client = MagicMock()
        client.database.get_synced_database_table.return_value = _synced(
            state="PROVISIONING"
        )
        sleeps: list = []
        mgr = _make_manager(client, sleeps=sleeps)
        # Force the deadline check to fire after a few iterations by stubbing
        # ``time.time`` in the manager's module. Extra ``time.time()`` calls can
        # occur (SDK idle wait, remaining budget); tail with a repeat so the
        # iterator never exhausts during the full suite.
        import importlib

        mod = importlib.import_module(
            "back.core.graphdb.lakebase.SyncedTableManager"
        )
        times = iter(chain([0.0, 1.0, 2.0], repeat(999.0)))
        monkeypatch.setattr(mod.time, "time", lambda: next(times))
        with pytest.raises(InfrastructureError, match="Timed out"):
            mgr.wait_for_completion("cat.sch.tab", timeout_s=5)


# ---------------------------------------------------------------
# delete
# ---------------------------------------------------------------


class TestDelete:
    def test_delete_calls_sdk(self):
        client = MagicMock()
        mgr = _make_manager(client)
        mgr.delete("cat.sch.tab", purge_data=True)
        client.database.delete_synced_database_table.assert_called_once_with(
            name="cat.sch.tab", purge_data=True
        )

    def test_delete_not_found_is_silent(self):
        client = MagicMock()
        client.database.delete_synced_database_table.side_effect = RuntimeError(
            "404 not found"
        )
        mgr = _make_manager(client)
        # Should not raise.
        mgr.delete("cat.sch.tab")

    def test_delete_other_error_wrapped(self):
        client = MagicMock()
        client.database.delete_synced_database_table.side_effect = RuntimeError(
            "permission denied"
        )
        mgr = _make_manager(client)
        with pytest.raises(InfrastructureError):
            mgr.delete("cat.sch.tab")
