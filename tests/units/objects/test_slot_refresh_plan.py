"""``TwinSlotRefreshPlan``: when a slot pipeline update must be a full refresh."""

from __future__ import annotations

import pytest

from back.core.errors import InfrastructureError
from back.objects.digitaltwin.build import TwinSlotRefreshPlan

pytestmark = pytest.mark.unit

_SRC = "main.sales.customers"


def _run(**facts) -> dict:
    return {"status": "success", "stats": {"materialization": facts}}


class _Warehouse:
    """``DESCRIBE HISTORY`` / ``DESCRIBE DETAIL`` of the sources from canned answers."""

    def __init__(self, *, version=7, table_id="", history=(), fail_history=False):
        self.version, self.table_id = version, table_id
        self.history, self.fail_history = list(history), fail_history
        self.calls: list[str] = []

    def execute_query(self, sql):
        self.calls.append(sql)
        if sql.startswith("DESCRIBE DETAIL"):
            return [{"id": self.table_id}] if self.table_id else []
        if sql.endswith(" LIMIT 1"):
            return [{"version": self.version}]
        if self.fail_history:
            raise RuntimeError("PERMISSION_DENIED")
        return self.history


class TestOwed:
    def test_a_recorded_drift_is_owed(self):
        assert TwinSlotRefreshPlan.owed([_run(reconcile_drift=True)]) == "reconcile_drift"

    def test_a_pending_refresh_is_owed_with_its_reason(self):
        runs = [_run(slot_full_refresh_pending=True, slot_full_refresh_reason="source_overwritten")]

        assert TwinSlotRefreshPlan.owed(runs) == "source_overwritten"

    def test_a_completed_refresh_settles_it(self):
        runs = [_run(slot_full_refresh_pending=False), _run(reconcile_drift=True)]

        assert TwinSlotRefreshPlan.owed(runs) == ""

    @pytest.mark.parametrize(
        "newer",
        [_run(strategy="merge"), _run(), {"status": "error"}, _run(slot_full_refresh_pending=None)],
        ids=["merge-build", "no-slot-facts", "no-stats", "full-build"],
    )
    def test_runs_without_a_slot_update_are_passed_over(self, newer):
        assert TwinSlotRefreshPlan.owed([newer, _run(reconcile_drift=True)]) == "reconcile_drift"

    def test_nothing_recorded_owes_nothing(self):
        assert TwinSlotRefreshPlan.owed([]) == ""


class TestPreviousSourceState:
    def test_the_newest_run_with_versions(self):
        runs = [
            _run(source_versions={}),
            _run(source_versions={_SRC: 4}, source_table_ids={_SRC: "id-1"}),
            _run(source_versions={_SRC: 2}),
        ]

        assert TwinSlotRefreshPlan.previous_source_state(runs) == {
            "source_versions": {_SRC: 4},
            "source_table_ids": {_SRC: "id-1"},
        }

    def test_unusable_versions_are_dropped(self):
        runs = [_run(source_versions={_SRC: "4", "x.y.z": True, "a.b.c": 1})]

        assert TwinSlotRefreshPlan.previous_source_state(runs)["source_versions"] == {"a.b.c": 1}

    def test_no_history(self):
        assert TwinSlotRefreshPlan.previous_source_state("garbage") == {
            "source_versions": {},
            "source_table_ids": {},
        }


class TestPlan:
    def test_no_rewrite_no_debt_is_an_incremental_update(self):
        history = [{"version": 7, "operation": "MERGE"}, {"version": 6, "operation": "MERGE"}]
        warehouse = _Warehouse(version=7, table_id="id-1", history=history)
        runs = [_run(source_versions={_SRC: 5}, source_table_ids={_SRC: "id-1"})]

        plan = TwinSlotRefreshPlan.plan(warehouse, runs, [_SRC])

        assert plan == {
            "reason": "",
            "before": {
                "source_versions": {_SRC: 5},
                "source_table_ids": {_SRC: "id-1"},
                "slot_full_refresh_pending": False,
                "slot_full_refresh_reason": "",
            },
            "after": {
                "source_versions": {_SRC: 7},
                "source_table_ids": {_SRC: "id-1"},
                "slot_full_refresh_pending": False,
                "slot_full_refresh_reason": "",
            },
        }
        assert f"DESCRIBE HISTORY {_SRC} LIMIT 3" in warehouse.calls

    def test_a_rewrite_is_pending_until_the_update_completes(self):
        warehouse = _Warehouse(history=[{"version": 6, "operation": "TRUNCATE"}])

        plan = TwinSlotRefreshPlan.plan(warehouse, [_run(source_versions={_SRC: 5})], [_SRC])

        assert plan["reason"] == "source_overwritten"
        assert plan["before"]["slot_full_refresh_pending"] is True
        assert plan["before"]["source_versions"] == {_SRC: 5}
        assert plan["after"]["slot_full_refresh_pending"] is False

    def test_another_table_id_is_a_rewrite_without_reading_the_history(self):
        warehouse = _Warehouse(version=5, table_id="id-2")
        runs = [_run(source_versions={_SRC: 5}, source_table_ids={_SRC: "id-1"})]

        assert TwinSlotRefreshPlan.plan(warehouse, runs, [_SRC])["reason"] == "source_overwritten"

    def test_an_owed_drift_wins_over_a_rewrite(self):
        warehouse = _Warehouse(history=[{"version": 6, "operation": "TRUNCATE"}])
        runs = [_run(reconcile_drift=True, source_versions={_SRC: 5})]

        assert TwinSlotRefreshPlan.plan(warehouse, runs, [_SRC])["reason"] == "reconcile_drift"

    def test_an_unreadable_history_raises(self):
        warehouse = _Warehouse(fail_history=True)

        with pytest.raises(InfrastructureError):
            TwinSlotRefreshPlan.plan(warehouse, [_run(source_versions={_SRC: 5})], [_SRC])

    def test_without_sources_only_a_drift_counts(self):
        plan = TwinSlotRefreshPlan.plan(_Warehouse(), [_run(reconcile_drift=True)], [])

        assert plan["reason"] == "reconcile_drift"
        assert plan["after"]["source_versions"] == {}
