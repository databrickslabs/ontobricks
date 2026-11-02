"""SlotReconciler: counts and triple hashes of ``_slots`` versus ``_data``."""

from __future__ import annotations

from decimal import Decimal

import pytest

from back.core.errors import InfrastructureError, ValidationError
from back.core.streaming import SlotReconciler

pytestmark = pytest.mark.unit

_SLOTS = "main.kg.triplestore_sales_V1_slots"
_DATA = "main.kg.triplestore_sales_V1_data"


class _Client:
    """Answers the totals / per-map queries from canned rows; records the SQL."""

    def __init__(self, *, totals=None, per_map=None, fail: bool = False):
        self.calls: list[str] = []
        self._totals = totals or {}
        self._per_map = per_map or []
        self._fail = fail

    def execute_query(self, sql: str):
        self.calls.append(sql)
        if self._fail:
            raise RuntimeError("TABLE_OR_VIEW_NOT_FOUND")
        if "GROUP BY map_id" in sql:
            return list(self._per_map)
        for fqn, row in self._totals.items():
            if f"FROM {fqn})" in sql:
                return [row]
        return [{"n": 0, "h": None}]


class TestCountsSql:
    def test_the_totals_count_distinct_triples(self):
        sql = SlotReconciler.counts_sql(_DATA)

        assert sql == (
            "SELECT count(*) AS n, "
            "sum(CAST(xxhash64(subject, predicate, object) AS DECIMAL(38, 0))) AS h "
            f"FROM (SELECT DISTINCT subject, predicate, object FROM {_DATA})"
        )

    def test_the_per_mapping_counts_group_by_map_id(self):
        sql = SlotReconciler.counts_sql(_SLOTS, group_by_map_id=True)

        assert sql == (
            "SELECT map_id, count(*) AS n, "
            "sum(CAST(xxhash64(subject, predicate, object) AS DECIMAL(38, 0))) AS h "
            f"FROM {_SLOTS} GROUP BY map_id ORDER BY map_id"
        )

    @pytest.mark.parametrize("fqn", ["", "main.kg.t; DROP TABLE x", "main.kg.t'--"])
    def test_an_unsafe_relation_name_is_refused(self, fqn):
        with pytest.raises(ValidationError):
            SlotReconciler.counts_sql(fqn)


class TestCompare:
    def test_equal_counts_and_hashes_mean_no_drift(self):
        client = _Client(totals={_SLOTS: {"n": 3, "h": 42}, _DATA: {"n": 3, "h": 42}})

        result = SlotReconciler.compare(client, _SLOTS, _DATA)

        assert result["drift"] is False
        assert result["slots"] == {"rows": 3, "hash": 42}
        assert result["data"] == {"rows": 3, "hash": 42}

    def test_a_row_count_mismatch_is_drift(self):
        client = _Client(totals={_SLOTS: {"n": 3, "h": 42}, _DATA: {"n": 2, "h": 42}})

        assert SlotReconciler.compare(client, _SLOTS, _DATA)["drift"] is True

    def test_a_hash_only_mismatch_is_drift(self):
        client = _Client(totals={_SLOTS: {"n": 3, "h": 42}, _DATA: {"n": 3, "h": 43}})

        assert SlotReconciler.compare(client, _SLOTS, _DATA)["drift"] is True

    def test_an_empty_relation_hashes_to_zero(self):
        client = _Client(totals={_SLOTS: {"n": 0, "h": None}, _DATA: {"n": 0, "h": None}})

        result = SlotReconciler.compare(client, _SLOTS, _DATA)

        assert result["slots"] == {"rows": 0, "hash": 0}
        assert result["drift"] is False

    def test_decimal_sums_are_returned_as_integers(self):
        big = Decimal("184467440737095516150")
        client = _Client(totals={_SLOTS: {"n": 1, "h": big}, _DATA: {"n": 1, "h": str(big)}})

        result = SlotReconciler.compare(client, _SLOTS, _DATA)

        assert result["slots"]["hash"] == 184467440737095516150
        assert result["drift"] is False

    def test_the_slots_are_also_listed_per_mapping(self):
        client = _Client(
            totals={_SLOTS: {"n": 3, "h": 1}, _DATA: {"n": 3, "h": 1}},
            per_map=[{"map_id": "map_customer", "n": 2, "h": 5}, {"map_id": "map_order", "n": 1, "h": -4}],
        )

        result = SlotReconciler.compare(client, _SLOTS, _DATA)

        assert result["by_map_id"] == [
            {"map_id": "map_customer", "rows": 2, "hash": 5},
            {"map_id": "map_order", "rows": 1, "hash": -4},
        ]
        assert any("GROUP BY map_id" in sql and _SLOTS in sql for sql in client.calls)

    def test_a_warehouse_error_is_an_infrastructure_error(self):
        with pytest.raises(InfrastructureError, match="Could not count"):
            SlotReconciler.compare(_Client(fail=True), _SLOTS, _DATA)
