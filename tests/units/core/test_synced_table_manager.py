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
from back.core.graphdb.lakebase import SyncedTableRegistration
from back.core.graphdb.lakebase.SyncedTableManager import SyncedTableManager
from databricks.sdk.service.database import SyncedTableSchedulingPolicy
from databricks.sdk.service.pipelines import StartUpdateCause

pytestmark = pytest.mark.unit


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


def _synced(
    state: str = "ONLINE_NO_PENDING_UPDATE",
    pipeline_id: str = "pid-1",
    *,
    source: Any = None,
    policy: Any = None,
) -> Any:
    """A synced-table GET response; ``spec`` is attached only when asked for."""
    synced = SimpleNamespace(
        data_synchronization_status=SimpleNamespace(
            detailed_state=state,
            pipeline_id=pipeline_id,
        )
    )
    if source is not None or policy is not None:
        synced.spec = SimpleNamespace(source_table_full_name=source, scheduling_policy=policy)
    return synced


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
            primary_key_columns=["subject", "predicate", "object_hash"],
        )
        assert out is not None
        client.database.create_synced_database_table.assert_not_called()

    def test_ensure_uses_fallback_when_failed_registration_cannot_be_deleted(self):
        client = MagicMock()
        failed = _synced(state="OFFLINE_FAILED", pipeline_id="missing-pipeline")
        fallback = _synced()
        fallback.name = "cat.sch.tab_b"
        client.database.get_synced_database_table.side_effect = [
            failed,
            None,
            fallback,
        ]
        client.database.delete_synced_database_table.side_effect = RuntimeError(
            "control-plane delete failed"
        )
        mgr = _make_manager(client)

        out = mgr.ensure(
            "cat.sch.tab",
            source_table_full_name="cat.sch.view",
            primary_key_columns=["subject", "predicate", "object_hash"],
        )

        assert out.name == "cat.sch.tab_b"
        created = client.database.create_synced_database_table.call_args.kwargs[
            "synced_table"
        ]
        assert created["name"] == "cat.sch.tab_b"

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
                primary_key_columns=["subject", "predicate", "object_hash"],
                sync_mode="snapshot",
            )
        client.database.create_synced_database_table.assert_called_once()
        assert captured["payload"]["name"] == "cat.sch.tab"
        assert captured["payload"]["source_table_full_name"] == "cat.sch.view"
        assert captured["payload"]["primary_key_columns"] == [
            "subject",
            "predicate",
            "object_hash",
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
                primary_key_columns=["subject", "predicate", "object_hash"],
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

    def test_wait_for_completion_detects_synced_table_failure_during_update(self):
        client = MagicMock()
        failed = _synced(state="OFFLINE_FAILED", pipeline_id="pid-1")
        failed.data_synchronization_status.message = "Pipeline pid-1 does not exist"
        client.database.get_synced_database_table.side_effect = [
            _synced(state="ONLINE_TRIGGERED_UPDATE"),
            failed,
        ]
        client.pipelines.get_update.return_value = SimpleNamespace(
            update=SimpleNamespace(state=SimpleNamespace(name="RUNNING"))
        )
        mgr = _make_manager(client)

        with pytest.raises(
            InfrastructureError,
            match="OFFLINE_FAILED.*Pipeline pid-1 does not exist",
        ):
            mgr.wait_for_completion(
                "cat.sch.tab",
                timeout_s=60,
                pipeline_update_id="upd-1",
            )

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


# ---------------------------------------------------------------
# ensure -- a changed definition is deleted and recreated
# ---------------------------------------------------------------

_PK = ["subject", "predicate", "object_hash"]


def _created_spec(client: MagicMock) -> dict:
    return client.database.create_synced_database_table.call_args.kwargs["synced_table"]["spec"]


class TestEnsureSpecDrift:
    """A synced table's definition cannot change in place: drift means delete + create."""

    def test_a_changed_source_deletes_and_recreates_the_registration(self):
        client = MagicMock()
        client.database.get_synced_database_table.side_effect = [
            _synced(source="cat.sch.view", policy="SNAPSHOT"),
            _synced(source="cat.sch.view_data", policy="SNAPSHOT"),
        ]
        mgr = _make_manager(client)

        mgr.ensure(
            "cat.sch.tab",
            source_table_full_name="cat.sch.view_data",
            primary_key_columns=_PK,
        )

        client.database.delete_synced_database_table.assert_called_once_with(
            name="cat.sch.tab", purge_data=True
        )
        assert _created_spec(client)["source_table_full_name"] == "cat.sch.view_data"

    def test_a_changed_schedule_deletes_and_recreates_the_registration(self):
        client = MagicMock()
        client.database.get_synced_database_table.side_effect = [
            _synced(source="cat.sch.slots", policy=SyncedTableSchedulingPolicy.SNAPSHOT),
            _synced(source="cat.sch.slots", policy=SyncedTableSchedulingPolicy.TRIGGERED),
        ]
        mgr = _make_manager(client)

        mgr.ensure(
            "cat.sch.tab",
            source_table_full_name="cat.sch.slots",
            primary_key_columns=["map_id", "source_key", "predicate"],
            sync_mode="triggered",
        )

        client.database.delete_synced_database_table.assert_called_once()
        assert _created_spec(client)["scheduling_policy"] == "TRIGGERED"

    def test_an_unchanged_definition_is_reused(self):
        """Source names compare case-insensitively; the SDK enum matches its string."""
        client = MagicMock()
        client.database.get_synced_database_table.return_value = _synced(
            source="cat.sch.triplestore_dom_v3_data",
            policy=SyncedTableSchedulingPolicy.CONTINUOUS,
        )
        mgr = _make_manager(client)

        mgr.ensure(
            "cat.sch.tab",
            source_table_full_name="cat.sch.triplestore_dom_V3_data",
            primary_key_columns=_PK,
            sync_mode="continuous",
        )

        client.database.delete_synced_database_table.assert_not_called()
        client.database.create_synced_database_table.assert_not_called()

    def test_a_drifted_fallback_registration_is_recreated_under_its_name(self):
        ghost = RuntimeError("Destination table g_sync already exists in schema ontobricks")
        client = MagicMock()
        client.database.get_synced_database_table.side_effect = [
            None,  # primary: ghost reservation, no UC registration
            _synced(source="cat.sch.view", policy="SNAPSHOT"),  # fallback _b, old source
            _synced(source="cat.sch.view_data", policy="SNAPSHOT"),  # _b after recreate
        ]
        client.database.create_synced_database_table.side_effect = [ghost, ghost, None]
        mgr = _make_manager(client)

        out = mgr.ensure(
            "cat.sch.tab",
            source_table_full_name="cat.sch.view_data",
            primary_key_columns=_PK,
        )

        assert out.spec.source_table_full_name == "cat.sch.view_data"
        deleted = [
            c.kwargs["name"] for c in client.database.delete_synced_database_table.call_args_list
        ]
        assert deleted == ["cat.sch.tab", "cat.sch.tab_b"]
        created = client.database.create_synced_database_table.call_args.kwargs["synced_table"]
        assert created["name"] == "cat.sch.tab_b"

    def test_the_hook_runs_right_before_a_drifted_registration_is_deleted(self):
        client = MagicMock()
        client.database.get_synced_database_table.side_effect = [
            _synced(source="cat.sch.view", policy="SNAPSHOT"),
            _synced(source="cat.sch.view_data", policy="SNAPSHOT"),
        ]
        events: list = []
        client.database.delete_synced_database_table.side_effect = (
            lambda **kw: events.append(("delete", kw["name"]))
        )
        mgr = _make_manager(client)

        mgr.ensure(
            "cat.sch.tab",
            source_table_full_name="cat.sch.view_data",
            primary_key_columns=_PK,
            before_delete=lambda name: events.append(("hook", name)),
        )

        assert events == [("hook", "cat.sch.tab"), ("delete", "cat.sch.tab")]

    def test_the_hook_is_not_called_for_an_unchanged_registration(self):
        client = MagicMock()
        client.database.get_synced_database_table.return_value = _synced(
            source="cat.sch.view_data", policy="SNAPSHOT"
        )
        hook = MagicMock()
        mgr = _make_manager(client)

        mgr.ensure(
            "cat.sch.tab",
            source_table_full_name="cat.sch.view_data",
            primary_key_columns=_PK,
            before_delete=hook,
        )

        hook.assert_not_called()

    def test_the_drift_is_logged_at_info(self):
        client = MagicMock()
        client.database.get_synced_database_table.side_effect = [
            _synced(source="cat.sch.view", policy="SNAPSHOT"),
            _synced(source="cat.sch.view_data", policy="SNAPSHOT"),
        ]
        mgr = _make_manager(client)

        with patch("back.core.graphdb.lakebase.SyncedTableRegistration.logger") as logger:
            mgr.ensure(
                "cat.sch.tab",
                source_table_full_name="cat.sch.view_data",
                primary_key_columns=_PK,
            )

        messages = [c.args[0] % c.args[1:] for c in logger.info.call_args_list]
        assert any(
            "cat.sch.tab" in m and "cat.sch.view -> cat.sch.view_data" in m for m in messages
        )
        logger.warning.assert_not_called()


# ---------------------------------------------------------------
# trigger_and_wait -- continuous tables are self-managing
# ---------------------------------------------------------------


class _Clock:
    """Fake ``time.time`` that only advances when the manager sleeps."""

    def __init__(self) -> None:
        self.now = 1_000.0
        self.sleeps: list = []

    def time(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


def _continuous(state: str) -> Any:
    return _synced(state=state, source="cat.sch.data", policy="CONTINUOUS")


def _continuous_manager(client: MagicMock, clock: _Clock) -> SyncedTableManager:
    return SyncedTableManager(
        project_name="proj-123",
        branch_name="demo",
        client_factory=lambda: client,
        sleep=clock.sleep,
    )


class TestTriggerAndWaitSchedules:
    def test_a_continuous_table_is_waited_for_until_it_streams(self):
        client = MagicMock()
        client.database.get_synced_database_table.side_effect = [
            _continuous("PROVISIONING"),  # policy lookup
            _continuous("PROVISIONING"),
            _continuous("PROVISIONING_INITIAL_SNAPSHOT"),
            _continuous("ONLINE_CONTINUOUS_UPDATE"),
        ]
        clock = _Clock()
        mgr = _continuous_manager(client, clock)
        seen: list = []

        with patch("back.core.graphdb.lakebase.ContinuousSyncWait.time", clock):
            state = mgr.trigger_and_wait("cat.sch.tab", timeout_s=60, on_state_change=seen.append)

        assert state == "ONLINE_CONTINUOUS_UPDATE"
        assert seen == ["PROVISIONING", "PROVISIONING_INITIAL_SNAPSHOT", "ONLINE_CONTINUOUS_UPDATE"]
        assert len(clock.sleeps) == 2
        client.pipelines.start_update.assert_not_called()
        client.pipelines.wait_get_pipeline_idle.assert_not_called()

    def test_an_online_continuous_table_returns_without_sleeping(self):
        client = MagicMock()
        client.database.get_synced_database_table.return_value = _continuous("ONLINE")
        clock = _Clock()
        mgr = _continuous_manager(client, clock)

        with patch("back.core.graphdb.lakebase.ContinuousSyncWait.time", clock):
            assert mgr.trigger_and_wait("cat.sch.tab", timeout_s=60) == "ONLINE"

        assert clock.sleeps == []

    def test_a_failed_continuous_table_raises(self):
        client = MagicMock()
        client.database.get_synced_database_table.side_effect = [
            _continuous("PROVISIONING"),
            _continuous("PROVISIONING_INITIAL_SNAPSHOT"),
            _continuous("OFFLINE_FAILED"),
        ]
        clock = _Clock()
        mgr = _continuous_manager(client, clock)

        with patch("back.core.graphdb.lakebase.ContinuousSyncWait.time", clock):
            with pytest.raises(InfrastructureError, match="OFFLINE_FAILED"):
                mgr.trigger_and_wait("cat.sch.tab", timeout_s=60)

        client.pipelines.start_update.assert_not_called()

    def test_a_continuous_table_that_never_streams_times_out(self):
        client = MagicMock()
        client.database.get_synced_database_table.return_value = _continuous("PROVISIONING")
        clock = _Clock()
        mgr = _continuous_manager(client, clock)

        with patch("back.core.graphdb.lakebase.ContinuousSyncWait.time", clock):
            with pytest.raises(InfrastructureError, match="PROVISIONING"):
                mgr.trigger_and_wait("cat.sch.tab", timeout_s=12)

        assert sum(clock.sleeps) <= 12

    def test_a_cancelled_continuous_wait_stops(self):
        client = MagicMock()
        client.database.get_synced_database_table.return_value = _continuous("PROVISIONING")
        clock = _Clock()
        mgr = _continuous_manager(client, clock)

        with patch("back.core.graphdb.lakebase.ContinuousSyncWait.time", clock):
            with pytest.raises(OperationCancelledError):
                mgr.trigger_and_wait("cat.sch.tab", timeout_s=60, cancel_check=lambda: True)

    def test_a_snapshot_table_is_still_triggered(self):
        client = MagicMock()
        client.database.get_synced_database_table.return_value = _synced(
            state="ONLINE", source="cat.sch.view_data", policy="SNAPSHOT"
        )
        client.pipelines.start_update.return_value = SimpleNamespace(update_id="")
        mgr = _make_manager(client)

        assert mgr.trigger_and_wait("cat.sch.tab", timeout_s=60) == "ONLINE"
        client.pipelines.start_update.assert_called_once()


# ---------------------------------------------------------------
# SyncedTableRegistration -- reading the registered spec
# ---------------------------------------------------------------


class TestSyncedTableRegistration:
    @pytest.mark.parametrize(
        "policy",
        [SyncedTableSchedulingPolicy.TRIGGERED, "TRIGGERED", "triggered"],
        ids=["sdk-enum", "rest-string", "lower-case"],
    )
    def test_the_scheduling_policy_is_read_from_every_response_shape(self, policy):
        synced = _synced(source="cat.sch.t", policy=policy)
        assert SyncedTableRegistration.scheduling_policy(synced) == "TRIGGERED"

    def test_the_rest_namespace_shape_is_read(self):
        from back.core.graphdb.lakebase.SyncedTableManager import _DictNamespace

        synced = _DictNamespace(
            {"spec": {"source_table_full_name": "cat.sch.t", "scheduling_policy": "CONTINUOUS"}}
        )
        assert SyncedTableRegistration.source_table(synced) == "cat.sch.t"
        assert SyncedTableRegistration.scheduling_policy(synced) == "CONTINUOUS"

    @pytest.mark.parametrize(
        "synced",
        [None, SimpleNamespace(), MagicMock()],
        ids=["missing", "no-spec", "mock"],
    )
    def test_an_unreadable_spec_never_counts_as_drift(self, synced):
        assert SyncedTableRegistration.source_table(synced) == ""
        assert SyncedTableRegistration.scheduling_policy(synced) == ""
        assert SyncedTableRegistration.drift(synced, "cat.sch.other", "continuous") == ""

    def test_drift_describes_every_change(self):
        synced = _synced(source="cat.sch.view", policy="SNAPSHOT")

        drift = SyncedTableRegistration.drift(synced, "`cat`.`sch`.`slots`", "continuous")

        assert drift == (
            "source cat.sch.view -> `cat`.`sch`.`slots`, scheduling policy SNAPSHOT -> CONTINUOUS"
        )

    def test_backticks_and_case_do_not_count_as_drift(self):
        synced = _synced(source="cat.sch.view_data", policy="SNAPSHOT")

        assert SyncedTableRegistration.drift(synced, "`CAT`.`sch`.`View_Data`", "") == ""

    def test_discard_keeps_an_unchanged_registration(self):
        mgr = MagicMock()
        synced = _synced(source="cat.sch.t", policy="SNAPSHOT")

        kept = SyncedTableRegistration.discard_if_drifted(
            mgr, "cat.sch.tab", synced, "cat.sch.t", "snapshot"
        )

        assert kept is synced
        mgr.delete.assert_not_called()

    def test_discard_deletes_a_drifted_registration_with_its_data(self):
        mgr = MagicMock()
        synced = _synced(source="cat.sch.t", policy="SNAPSHOT")

        kept = SyncedTableRegistration.discard_if_drifted(
            mgr, "cat.sch.tab", synced, "cat.sch.t", "triggered"
        )

        assert kept is None
        mgr.delete.assert_called_once_with("cat.sch.tab", purge_data=True)
