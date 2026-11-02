"""A scheduled tick reconciles an in-place refreshed ``_data`` view and records drift."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from back.core.errors import InfrastructureError, NotFoundError
from back.core.graphdb.delta import materialize
from back.objects.digitaltwin.build import TwinSlotReconciliation
from back.objects.registry.schedule import build as scheduled_build

pytestmark = pytest.mark.unit

_DATA = "cat.sch.triplestore_dom_V3_data"
_SPEC = SimpleNamespace(source_tables=["main.sales.customers"])


@pytest.fixture
def names(monkeypatch):
    from back.core.graphdb.delta import _table_naming

    monkeypatch.setattr(_table_naming, "data_table_fqn", lambda d, s=None: _DATA)


def _reconcile(
    monkeypatch,
    *,
    kind="mv",
    result=None,
    error=None,
    store=None,
    spec=_SPEC,
    mode="incremental",
    sync_state="ONLINE",
):
    seen: dict = {}
    monkeypatch.setattr(materialize, "relation_kind", lambda client, fqn: kind)

    def run(domain, settings, client, *, get_store, recompute):
        seen.update(store=get_store(domain, settings) if get_store else None, recompute=recompute)
        if error is not None:
            raise error
        return result or {"reconcile_drift": False, "pairs": {}}

    monkeypatch.setattr(TwinSlotReconciliation, "run", staticmethod(run))
    synced = store if store is not None else SimpleNamespace(is_synced=True)
    drift = scheduled_build._reconcile_after_tick(
        synced,
        object(),
        SimpleNamespace(info={"materialization_mode": mode}),
        SimpleNamespace(),
        spec,
        "dom",
        sync_state=sync_state,
    )
    return drift, seen


class TestReconcileAfterTick:
    def test_drift_on_a_refreshed_view_is_reported_and_logged(self, monkeypatch, names):
        result = {"reconcile_drift": True, "pairs": {"view_data": True}}

        with patch.object(scheduled_build, "logger") as logger:
            drift, seen = _reconcile(monkeypatch, result=result)

        assert drift == {"reconcile_drift": True}
        assert seen["store"].is_synced is True and seen["recompute"] is True
        logger.warning.assert_called_once()

    def test_a_realtime_domain_compares_its_live_table_without_recompute(self, monkeypatch, names):
        result = {"reconcile_drift": False, "live_drift": True, "live": {"rows": 3}, "pairs": {}}

        with patch.object(scheduled_build, "logger") as logger:
            facts, seen = _reconcile(monkeypatch, mode="realtime", result=result, sync_state="")

        assert facts == {"live_drift": True}
        assert seen["recompute"] is False and seen["store"].is_synced is True
        logger.warning.assert_called_once()

    def test_a_realtime_drift_of_the_view_forces_no_slot_refresh(self, monkeypatch, names):
        result = {"reconcile_drift": True, "live_drift": False, "pairs": {"view_data": True}}

        assert _reconcile(monkeypatch, mode="realtime", result=result)[0] == {}

    def test_a_microbatch_domain_is_not_reconciled(self, monkeypatch, names):
        """Its slots run ahead of ``_data``: no pair can tell drift."""
        drift, seen = _reconcile(monkeypatch, mode="microbatch", result={"reconcile_drift": True})

        assert drift == {} and seen == {}

    def test_a_sync_that_did_not_come_online_is_not_compared(self, monkeypatch, names):
        _drift, seen = _reconcile(monkeypatch, sync_state="FAILED")

        assert seen["store"] is None

    def test_no_drift(self, monkeypatch, names):
        assert _reconcile(monkeypatch)[0] == {}

    def test_a_ctas_table_is_not_reconciled(self, monkeypatch, names):
        """The tick rebuilt ``_data`` by CTAS: it equals the VIEW by construction."""
        drift, seen = _reconcile(monkeypatch, kind="table", result={"reconcile_drift": True})

        assert drift == {} and seen == {}

    def test_a_full_domain_is_not_reconciled(self, monkeypatch, names):
        drift, seen = _reconcile(monkeypatch, spec=None, result={"reconcile_drift": True})

        assert drift == {} and seen == {}

    def test_an_unsynced_store_is_not_reconciled(self, monkeypatch, names):
        store = SimpleNamespace(is_synced=False)

        drift, seen = _reconcile(monkeypatch, store=store, result={"reconcile_drift": True})

        assert drift == {} and seen == {}

    @pytest.mark.parametrize(
        "error",
        [
            NotFoundError("no slots table"),
            InfrastructureError("warehouse down"),
            RuntimeError("driver bug"),
        ],
        ids=["no-slots", "warehouse", "unexpected"],
    )
    def test_a_failed_reconciliation_never_fails_the_tick(self, monkeypatch, names, error):
        with patch.object(scheduled_build, "logger") as logger:
            drift, _ = _reconcile(monkeypatch, error=error)

        assert drift == {}
        logger.warning.assert_called_once()


class TestTickRecordsDrift:
    def _run(self, monkeypatch, *, drift: bool) -> dict:
        monkeypatch.setattr(
            scheduled_build,
            "_generate_sql_from_r2rml",
            lambda d, n: ("SELECT 1", "cat.sch.triplestore_dom_V3", "g", "http://ex/", [], []),
        )
        monkeypatch.setattr(scheduled_build, "_slot_spec", lambda *a: _SPEC)
        monkeypatch.setattr(scheduled_build, "_write_graph_triples", lambda *a, **k: 4)
        monkeypatch.setattr(scheduled_build, "_persist_domain_metadata", lambda *a: None)
        monkeypatch.setattr(
            scheduled_build,
            "_reconcile_after_tick",
            lambda *a, **k: {"reconcile_drift": True} if drift else {},
        )
        src = MagicMock()
        src.create_or_replace_view.return_value = (True, "")
        ctx = SimpleNamespace(
            domain_name="dom",
            domain=SimpleNamespace(),
            loaded_version="3",
            scratch={},
            graph_store=SimpleNamespace(is_synced=True),
            warehouse_client=src,
            settings=SimpleNamespace(),
            registry_service=None,
            run_ts="2026-11-02T00:00:00+00:00",
            progress=lambda pct, msg: None,
            advance=lambda msg: None,
        )
        outcome = scheduled_build.run(ctx)
        assert outcome.status == "success"
        return ctx.scratch["stats"]

    def test_drift_is_recorded_for_the_next_build(self, monkeypatch):
        assert self._run(monkeypatch, drift=True)["materialization"] == {"reconcile_drift": True}

    def test_no_drift_records_no_materialization_facts(self, monkeypatch):
        assert "materialization" not in self._run(monkeypatch, drift=False)


class TestTickTrace:
    def _record(self, stats):
        from back.objects.registry.schedule.RunOutcome import RunOutcome

        registry = MagicMock()
        ctx = SimpleNamespace(
            _svc=object(),
            tm=None,
            task_id="t",
            domain_name="dom",
            version="3",
            run_ts="2026-11-02T00:00:00+00:00",
            scratch={"stats": stats},
            registry_service=registry,
        )
        scheduled_build.on_finish(ctx, RunOutcome(status="error", message="boom"), 1.0)
        return registry.record_build_run.call_args[0][1]["stats"]

    def test_a_tick_that_failed_before_any_fact_records_no_materialization(self):
        assert self._record({"ontology": {}, "materialization": {}}) == {"ontology": {}}

    def test_a_tick_that_left_a_refresh_pending_records_it(self):
        facts = {"slot_full_refresh_pending": True, "slot_full_refresh_reason": "reconcile_drift"}

        assert self._record({"materialization": facts})["materialization"] == facts
