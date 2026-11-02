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


def _reconcile(monkeypatch, *, kind="mv", result=None, error=None, store=None, spec=_SPEC):
    seen: dict = {}
    monkeypatch.setattr(materialize, "relation_kind", lambda client, fqn: kind)

    def run(domain, settings, client, *, get_store):
        seen["store"] = get_store(domain, settings)
        if error is not None:
            raise error
        return result or {"reconcile_drift": False, "pairs": {}}

    monkeypatch.setattr(TwinSlotReconciliation, "run", staticmethod(run))
    synced = store if store is not None else SimpleNamespace(is_synced=True)
    drift = scheduled_build._reconcile_after_tick(
        synced, object(), SimpleNamespace(), SimpleNamespace(), spec, "dom"
    )
    return drift, seen


class TestReconcileAfterTick:
    def test_drift_on_a_refreshed_view_is_reported_and_logged(self, monkeypatch, names):
        result = {"reconcile_drift": True, "pairs": {"view_data": True}}

        with patch.object(scheduled_build, "logger") as logger:
            drift, seen = _reconcile(monkeypatch, result=result)

        assert drift is True
        assert seen["store"].is_synced is True
        logger.warning.assert_called_once()

    def test_no_drift(self, monkeypatch, names):
        assert _reconcile(monkeypatch)[0] is False

    def test_a_ctas_table_is_not_reconciled(self, monkeypatch, names):
        """The tick rebuilt ``_data`` by CTAS: it equals the VIEW by construction."""
        drift, seen = _reconcile(monkeypatch, kind="table", result={"reconcile_drift": True})

        assert drift is False and seen == {}

    def test_a_full_domain_is_not_reconciled(self, monkeypatch, names):
        drift, seen = _reconcile(monkeypatch, spec=None, result={"reconcile_drift": True})

        assert drift is False and seen == {}

    def test_an_unsynced_store_is_not_reconciled(self, monkeypatch, names):
        store = SimpleNamespace(is_synced=False)

        drift, seen = _reconcile(monkeypatch, store=store, result={"reconcile_drift": True})

        assert drift is False and seen == {}

    @pytest.mark.parametrize(
        "error",
        [NotFoundError("no slots table"), InfrastructureError("warehouse down")],
        ids=["no-slots", "warehouse"],
    )
    def test_a_failed_reconciliation_never_fails_the_tick(self, monkeypatch, names, error):
        with patch.object(scheduled_build, "logger") as logger:
            drift, _ = _reconcile(monkeypatch, error=error)

        assert drift is False
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
        monkeypatch.setattr(scheduled_build, "_reconcile_after_tick", lambda *a: drift)
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
